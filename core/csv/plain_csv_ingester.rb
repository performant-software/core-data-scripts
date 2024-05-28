require 'active_support/all'
require 'csv'

require_relative './id_mapper'

module Csv
  class PlainCsvIngester
    def initialize(
      # The input folder
      input:,
      # The output folder
      output:,
      # The folder that contains the JSON ID maps
      id_map_path:,
      # Environment variable object
      env:,
      # Object with lists of fields (see existing scripts for examples)
      fields:,
      # List of models being imported (e.g. ['places', 'people'])
      model_files:,
      # Object with lists of user-defined fields on relations (see existing scripts for examples)
      relation_udfs: nil,
      # Name of the column in the input CSV(s) that contains the remote ID
      id_column: 'id',
      # Name of the column to read from for the ID map feature
      id_map_column: 'id'
    )
      @input_path = input
      @output_path = output
      @id_map_path = id_map_path
      @env = env
      @fields = fields
      @model_files = model_files
      @relation_udfs = relation_udfs
      @id_column = id_column
      @id_map_column = id_map_column

      if File.directory?(@output_path)
        FileUtils.remove_dir(@output_path)
      end

      Dir.mkdir(@output_path)

      @relationship_headers = [
        'project_model_relationship_id',
        'primary_record_uuid',
        'primary_record_type',
        'related_record_uuid',
        'related_record_type'
      ]

      if @relation_udfs && @relation_udfs.count > 0
        @relation_udfs.values.each do |udf_hash|
          udf_hash.keys.each do |key|
            @relationship_headers.push(key)
          end
        end
      end
    end

    # Set up the relationships file
    def init_relationships(relationships_path)
      File.write(
        relationships_path,
        "#{@relationship_headers.join(',')}\n"
      )
    end

    def parse_models
      @model_files.each do |filename|
        temp_file_path = "#{@output_path}/temp_#{filename}.csv"
        json_file_path = "#{@id_map_path}/#{filename}_map.json"

        mapper = IdMapper.new(
          csv_path: temp_file_path,
          json_path: json_file_path
        )

        id_map = mapper.get_hashmap

        CSV.open(temp_file_path, 'w') do |csv_out|
          table = CSV.read("#{@input_path}/#{filename}.csv", headers: true)

          # Set up header
          csv_out << [
            'project_model_id',
            'uuid',
            *@fields[filename.to_sym].keys,
            'original_id'
          ]

          table.each do |row|
            unless id_map[row[@id_map_column]]
              # If the source DB already has UUIDs, we can just use those.
              if row['uuid']
                id_map[row[@id_map_column]] = row['uuid']
              else
                id_map[row[@id_map_column]] = SecureRandom.uuid
              end
            end

            model_fields = @fields[filename.to_sym].values.map do |val|
              result = nil

              if val.class == Proc
                result = val.call(row) || nil
              elsif val.class == String
                result = row[val] || nil
              end

              result
            end

            csv_out << [
              @env["PROJECT_MODEL_ID_#{filename.upcase}"].to_i,
              id_map[row[@id_map_column]],
              *model_fields,
              row[@id_column]
            ]
          end
        end

        mapper.write_hashmap(id_map)
      end
    end

    def parse_relation(
      primary_model:,
      secondary_model:,
      primary_csv:,
      secondary_csv:,
      relation_csv:,
      primary_id_column:,
      secondary_id_column:,
      project_model_relation_id:,
      udfs: nil
    )
      relationships_path = "#{@output_path}/relationships.csv"

      unless File.exist?(relationships_path)
        init_relationships(relationships_path)
      end

      primary_table = CSV.read(primary_csv, headers: true)
      related_table = CSV.read(secondary_csv, headers: true)
      relations_table = CSV.read(relation_csv, headers: true)

      CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
        relations_table.each do |relation|
          matching_primary = primary_table.find { |ed| ed['original_id'] == relation[primary_id_column]}
          matching_related = related_table.find { |ed| ed['original_id'] == relation[secondary_id_column]}

          if matching_related && matching_primary
            new_relation = {}
            new_relation['project_model_relationship_id'] = project_model_relation_id.to_i
            new_relation['primary_record_uuid'] = matching_primary['uuid']
            new_relation['primary_record_type'] = primary_model
            new_relation['related_record_uuid'] = matching_related['uuid']
            new_relation['related_record_type'] = secondary_model

            if udfs
              udfs.keys.each do |key|
                if udfs[key].class == Proc
                  result = udfs[key].call(relation, matching_primary, matching_related) || nil
                elsif udfs[key].class == String
                  result = relation[udfs[key]] || nil
                end

                new_relation[key] = result
              end
            end

            result = order_relationship(new_relation)

            csv_out << result
          end
        end
      end
    end

    # Most relations are just simple stuff like "items_people". This method automatically
    # generates the long list of params for parse_relation in those cases.
    def parse_simple_relation(primary, related, primary_model = nil, related_model = nil)
      parse_relation(
        primary_model: primary_model || "CoreDataConnector::#{primary.singularize.capitalize}",
        secondary_model: related_model || "CoreDataConnector::#{related.singularize.capitalize}",
        primary_csv: "#{@output_path}/temp_#{primary}.csv",
        secondary_csv: "#{@output_path}/temp_#{related}.csv",
        primary_id_column: "#{primary}_id",
        secondary_id_column: "#{related}_id",
        relation_csv: "#{@input_path}/#{primary}_#{related}.csv",
        project_model_relation_id: @env["PROJECT_MODEL_RELATIONSHIP_ID_#{primary.upcase}_#{related.upcase}"],
        udfs: @relation_udfs ? @relation_udfs["#{primary}_#{related}".to_sym] : nil
      )
    end

    # Keeps us from having to worry about the
    # order of the keys in the obj we create
    def order_relationship(relation_obj)
      @relationship_headers.map { |field| relation_obj[field] }
    end

    def init_web_identifiers(web_identifiers_path)
      headers = [
        'web_authority_id',
        'identifiable_uuid',
        'identifiable_type',
        'identifier'
      ]

      File.write(
        web_identifiers_path,
        "#{headers.join(',')}\n"
      )
    end

    def parse_web_authority(
      # Name of the model, e.g. 'places'
      model:,
      # Name of the CSV column from which the IDs are being taken
      column:,
      # The ID of the authority in Core Data
      authority_id: nil,
      # The model being ingested (i.e. 'CoreDataConnector::Place')
      core_data_model: nil,
      # regex pattern for matching the ID within a longer string
      regex: nil
    )
      table = CSV.read("#{@output_path}/temp_#{model}.csv", headers: true)
      web_identifiers_path = "#{@output_path}/web_identifiers.csv"

      unless File.exist?(web_identifiers_path)
        init_web_identifiers(web_identifiers_path)
      end

      CSV.open(web_identifiers_path, 'a') do |csv_out|
        table.each do |record|
          if record[column] && record[column] != ''
            csv_out << [
              authority_id || @env["#{column.upcase}_AUTHORITY_ID"],
              record['uuid'],
              core_data_model || "CoreDataConnector::#{model.singularize.capitalize}",
              regex ? record[column][regex] : record[column]
            ]
          end
        end
      end
    end

    # Remove the original_id column, which was needed to
    # build relationships but confuses the CD importer.
    def cleanup(filenames, fields_to_remove = nil)
      fields = fields_to_remove || [
        'bnf',
        'dpla',
        'jisc',
        'original_id',
        'viaf',
        'wikidata'
      ]

      filenames.each do |filename|
        File.open("#{@output_path}/#{filename}.csv", 'w') do |file|
          temp_table = CSV.read("#{@output_path}/temp_#{filename}.csv", headers: true)

          fields.each do |field|
            temp_table.delete(field)
          end

          file.write(temp_table.to_csv)
        end

        File.delete("#{@output_path}/temp_#{filename}.csv")
      end
    end
  end
end
