require 'dotenv'
require 'optparse'

require_relative '../../core/csv/adapter'
require_relative '../../core/archive'

class GbofCsvImport < Csv::Adapter
  def cleanup
    super

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_places
    SQL
  end

  def load
    super

    export 'places.csv', <<-SQL.squish
      SELECT project_model_id,
             uuid,
             name,
             latitude,
             longitude,
             #{user_defined_column('PLACE_ADDRESS_UUID')}
        FROM z_places
       ORDER BY name
    SQL
  end

  def setup
    super

    execute <<-SQL.squish
      CREATE EXTENSION IF NOT EXISTS pgcrypto
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_places (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        project_model_id INTEGER,
        name VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
      )
    SQL
  end

  def transform
    super

    execute <<-SQL.squish
      INSERT INTO z_places ( project_model_id, name, latitude, longitude )
      SELECT #{env['PROJECT_MODEL_ID_PLACES'].to_i}, name, latitude, longitude
        FROM #{table_name}
      WHERE ( latitude IS NOT NULL AND longitude IS NOT NULL )
    SQL
  end

  protected

  def column_names
    [{
      name: 'Item Id',
      type: 'INTEGER'
    },
    {
      name: 'Item URI',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Title',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Subject',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Description',
      type: 'TEXT'
    },
    {
      name: 'Dublin Core:Creator',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Source',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Publisher',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Date',
      type: 'INTEGER'
    },
    {
      name: 'Dublin Core:Contributor',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Rights',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Format',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Language',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Type',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Identifier',
      type: 'VARCHAR'
    },
    {
      name: 'Dublin Core:Coverage',
      type: 'VARCHAR'
    },
    {
      name: 'Item Type Metadata:geolocation:address',
      type: 'VARCHAR'
    },
    {
      name: 'Item Type Metadata:geolocation:zoom_level',
      type: 'INTEGER'
    },
    {
      name: 'Item Type Metadata:geolocation:longitude',
      type: 'DECIMAL'
    },
    {
      name: 'Item Type Metadata:geolocation:latitude',
      type: 'DECIMAL'
    },
    {
      name: 'Item Type Metadata:County',
      type: 'VARCHAR'
    },
    {
      name: 'Item Type Metadata:Elev_f',
      type: 'INTEGER'
    },
    {
      name: 'Item Type Metadata:Elev_m',
      type: 'INTEGER'
    },
    {
      name: 'Item Type Metadata:TopoName',
      type: 'VARCHAR'
    },
    {
      name: 'Item Type Metadata:Coordinates',
      type: 'VARCHAR'
    },
    {
      name: 'Item Type Metadata:Identifier',
      type: 'INTEGER'
    },
    {
      name: 'Item Type Metadata:URL',
      type: 'VARCHAR'
    },
    {
      name: 'tags',
      type: 'VARCHAR'
    },
    {
      name: 'itemType',
      type: 'VARCHAR'
    },
    {
      name: 'collection',
      type: 'VARCHAR'
    },
    {
      name: 'public',
      type: 'BOOLEAN'
    },
    {
      name: 'featured',
      type: 'BOOLEAN'
    }]
  end
end

# Parse environment variables
env = Dotenv.parse './scripts/gbof/.env.development'

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-d DATABASE', '--database DATABASE', 'Database name'
  opts.on '-u USER', '--user USER', 'Database username'
  opts.on '-f FILE', '--file FILE', 'Source filepath'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
end.parse!(into: options)

# Run the importer
import = CsvImport.new(
  database: options[:database],
  user: options[:user],
  filepath: options[:file],
  output: options[:output],
  env: env
)

import.setup
import.extract
import.transform
import.load
import.cleanup

filepaths = [
  '#{options[:output]}/places.csv'
]

archive = Archive.new
archive.create_archive(filepaths, options[:output])
