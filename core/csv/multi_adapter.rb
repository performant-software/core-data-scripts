require_relative 'adapter'

module Csv
  class MultiAdapter < Adapter
    attr_reader :connection, :files, :output_path, :env

    def initialize(database:, user:, input:, output:, env:)
      @output_path = output
      @env = env

      @connection = PG.connect(dbname: database, user: user)
      @files = initialize_files(input)
    end

    def cleanup
      files.each do |file|
        table_name = file[:table_name]

        execute <<-SQL.squish
          DROP TABLE IF EXISTS #{table_name}
        SQL
      end
    end

    def extract
      files.each do |file|
        file_name = file[:file_name]
        file_path = file[:file_path]
        table_name = file[:table_name]

        columns = column_names(file_name)
                    .map{ |column| column[:name] }
                    .join(', ')

        execute <<-SQL.squish
          COPY #{table_name} (#{columns}) FROM '#{file_path}' DELIMITERS ',' CSV HEADER;
        SQL
      end
    end

    def setup
      files.each do |file|
        file_name = file[:file_name]
        table_name = file[:table_name]

        execute <<-SQL.squish
          DROP TABLE IF EXISTS #{table_name}
        SQL

        puts file_name

        columns = column_names(file_name)
                    .map{ |column| "#{column[:name]} #{column[:type]}" }
                    .join(', ')

        execute <<-SQL.squish
          CREATE TABLE #{table_name} (
            id SERIAL,
            #{columns}
          )   
        SQL
      end
    end

    protected

    def column_names(file_name)
      []
    end

    private

    def initialize_files(dir)
      files = []

      Dir.children(dir).each do |file_name|
        next unless File.extname(file_name) == '.csv'

        file_path = File.join(dir, file_name)
        table_name = File.basename(file_name, '.csv').downcase.gsub(' ', '_')

        files << {
          file_name: file_name,
          file_path: file_path,
          table_name: table_name
        }
      end

      files
    end
  end
end