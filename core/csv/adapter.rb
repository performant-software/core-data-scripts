require 'active_support/core_ext/string/filters'
require 'pg'

module Csv
  class Adapter
    attr_reader :connection, :filepath, :output_path, :env

    def initialize(database:, filepath:, output:, env:)
      @connection = PG.connect(dbname: database, user: 'postgres')
      @filepath = filepath
      @output_path = output
      @env = env
    end

    def cleanup
      execute <<-SQL.squish
        DROP TABLE IF EXISTS #{table_name}
      SQL
    end

    def extract
      columns = column_names
                  .map{ |column| column[:name] }
                  .join(', ')

      execute <<-SQL.squish
        COPY #{table_name} (#{columns}) FROM '#{filepath}' DELIMITERS ',' CSV HEADER;
      SQL
    end

    def load

    end

    def setup
      execute <<-SQL.squish
        DROP TABLE IF EXISTS #{table_name}
      SQL

      columns = column_names
                  .map{ |column| "#{column[:name]} #{column[:type]}" }
                  .join(', ')

      execute <<-SQL.squish
        CREATE TABLE #{table_name} (
          id SERIAL,
          #{columns}
        )   
      SQL
    end

    def transform

    end

    protected

    def additional_column_names
      []
    end

    def column_names
      []
    end

    def execute(sql)
      connection.exec sql
    end

    def export(filename, query)
      execute <<-SQL.squish
        COPY ( #{query} ) TO '#{output_path}/#{filename}' DELIMITER ',' CSV HEADER
      SQL
    end

    def table_name
      'z_temp'
    end
  end
end