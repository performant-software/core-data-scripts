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
      # Environment variable object
      env:,
      # Object with lists of fields (see existing scripts for examples)
      fields:,
      # List of models being imported (e.g. ['places', 'people'])
      model_files:,
      # Object with lists of user-defined fields on relations (see existing scripts for examples)
      relation_udfs: nil,
      # Name of the column in the input CSV(s) that contains the remote ID
      id_column: 'id'
    )
      @input_path = input
      @output_path = output
      @env = env
      @fields = fields
      @model_files = model_files
      @relation_udfs = relation_udfs
      @id_column = id_column

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
    def init_relationships
      File.write(
        "#{@output_path}/relationships.csv",
        "#{@relationship_headers.join(',')}\n"
      )
    end

    def parse_models
      @model_files.each do |filename|
        temp_file_path = "#{@output_path}/temp_#{filename}.csv"
        json_file_path = "#{@output_path}/#{filename}_map.json"

        mapper = IdMapper.new(
          csv_path: temp_file_path,
          json_path: json_file_path,
          id_column: @id_column
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
            unless id_map[row[@id_column]]
              id_map[row[@id_column]] = SecureRandom.uuid
            end

            csv_out << [
              @env["PROJECT_MODEL_ID_#{filename.upcase}"].to_i,
              id_map[row[@id_column]],
              *@fields[filename.to_sym].values.map { |val| row[val] ? row[val].gsub(/\A\p{Space}*/, '') : nil },
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
                new_relation[key] = relation[udfs[key]]
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

    # Remove the original_id column, which was needed to
    # build relationships but confuses the CD importer.
    def cleanup(filenames, fields = ['original_id'])
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
