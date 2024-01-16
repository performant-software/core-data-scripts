require 'active_support/all'
require 'csv'

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
        CSV.open("#{@output_path}/temp_#{filename}.csv", 'w') do |csv_out|
          table = CSV.read("#{@input_path}/#{filename}.csv", headers: true)

          # Set up header
          csv_out << [
            'project_model_id',
            'uuid',
            *@fields[filename.to_sym].keys,
            'original_id'
          ]

          table.each do |row|
            csv_out << [
              @env["PROJECT_MODEL_ID_#{filename.upcase}"].to_i,
              SecureRandom.uuid,
              *@fields[filename.to_sym].values.map { |val| val == nil ? val : row[val] },
              row[@id_column]
            ]
          end
        end
      end
    end

    def parse_relation(primary, related, primary_model = nil, related_model = nil)
      primary_table = CSV.read("#{@output_path}/temp_#{primary}.csv", headers: true)
      related_table = CSV.read("#{@output_path}/temp_#{related}.csv", headers: true)
      relations = CSV.read("#{@input_path}/#{primary}_#{related}.csv", headers: true)

      primary_id_column = "#{primary}_id"
      related_id_column = "#{related}_id"

      # Accept model name arguments for cases where the model name is different from the file name
      primary_model_name = primary_model || "CoreDataConnector::#{primary.singularize.capitalize}"
      related_model_name = related_model || "CoreDataConnector::#{related.singularize.capitalize}"

      # Get the env variable that the relationship ID is stored in
      env_var = "PROJECT_MODEL_RELATIONSHIP_ID_#{primary.upcase}_#{related.upcase}"

      CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
        relations.each do |relation|
          matching_primary = primary_table.find { |ed| ed['original_id'] == relation[primary_id_column]}
          matching_related = related_table.find { |ed| ed['original_id'] == relation[related_id_column]}

          if matching_related && matching_primary
            new_relation = {}
            new_relation['project_model_relationship_id'] = @env[env_var].to_i
            new_relation['primary_record_uuid'] = matching_primary['uuid']
            new_relation['primary_record_type'] = primary_model_name
            new_relation['related_record_uuid'] = matching_related['uuid']
            new_relation['related_record_type'] = related_model_name

            user_defined_fields = @relation_udfs["#{primary}_#{related}".to_sym]

            if user_defined_fields
              user_defined_fields.keys.each do |key|
                new_relation[key] = relation[user_defined_fields[key]]
              end
            end

            result = order_relationship(new_relation)

            csv_out << result
          end
        end
      end
    end

    # Keeps us from having to worry about the
    # order of the keys in the obj we create
    def order_relationship(relation_obj)
      @relationship_headers.map { |field| relation_obj[field] }
    end

    # Remove the original_id column, which was needed to
    # build relationships but confuses the CD importer.
    def cleanup(filenames)
      filenames.each do |filename|
        File.open("#{@output_path}/#{filename}.csv", 'w') do |file|
          temp_table = CSV.read("#{@output_path}/temp_#{filename}.csv", headers: true)
          temp_table.delete('original_id')
          file.write(temp_table.to_csv)
        end

        File.delete("#{@output_path}/temp_#{filename}.csv")
      end
    end
  end
end
