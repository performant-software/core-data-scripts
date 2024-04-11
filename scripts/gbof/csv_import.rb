require 'active_support/core_ext/string/filters'
require 'dotenv'
require 'optparse'

require_relative '../../core/csv/adapter'
require_relative '../../core/archive'
require_relative '../../core/env'

class CsvImport < Csv::Adapter
  def cleanup
    super

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_organizations
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_people
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_places
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_relationships
    SQL
  end

  def load
    super

    export 'organizations.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             name AS name, 
             NULL AS description,
             #{user_defined_column('ORGANIZATION_DATES_UUID')},
             #{user_defined_column('ORGANIZATION_ARCHIVES_UUID')},
             #{user_defined_column('ORGANIZATION_NOTES_UUID')},
             #{user_defined_column('ORGANIZATION_OCLCNO_UUID')},
             #{user_defined_column('ORGANIZATION_LIBRARIES_UUID')}
        FROM z_organizations
       ORDER BY name
    SQL

    export 'people.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             last_name, 
             first_name, 
             NULL AS middle_name, 
             NULL AS biography
        FROM z_people
       ORDER BY last_name, first_name
    SQL

    export 'places.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             name, 
             latitude, 
             longitude
        FROM z_places
       ORDER BY name
    SQL

    export 'relationships.csv', <<-SQL.squish
      SELECT project_model_relationship_id, 
             primary_record_uuid, 
             primary_record_type, 
             related_record_uuid, 
             related_record_type
        FROM z_relationships
    SQL
  end

  def setup
    super

    execute <<-SQL.squish
      CREATE EXTENSION IF NOT EXISTS pgcrypto
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_organizations (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        project_model_id INTEGER,
        name VARCHAR,
        description VARCHAR,
        #{user_defined_column('ORGANIZATION_DATES_UUID')} VARCHAR,
        #{user_defined_column('ORGANIZATION_ARCHIVES_UUID')} VARCHAR,
        #{user_defined_column('ORGANIZATION_NOTES_UUID')} VARCHAR,
        #{user_defined_column('ORGANIZATION_OCLCNO_UUID')} VARCHAR,
        #{user_defined_column('ORGANIZATION_LIBRARIES_UUID')} VARCHAR
      )
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_people (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        project_model_id INTEGER,
        name VARCHAR,
        last_name VARCHAR,
        first_name VARCHAR
      )
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_places (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        project_model_id INTEGER,
        name VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL
      )
    SQL

    execute <<-SQL
      CREATE TABLE z_relationships (
        id SERIAL,
        project_model_relationship_id INTEGER,
        primary_record_uuid UUID,
        primary_record_type VARCHAR,
        related_record_uuid UUID,
        related_record_type VARCHAR
      )
    SQL
  end

  def transform
    super

    execute <<-SQL.squish
      INSERT INTO z_organizations ( 
        project_model_id, 
        name, 
        #{user_defined_column('ORGANIZATION_DATES_UUID')},
        #{user_defined_column('ORGANIZATION_ARCHIVES_UUID')},
        #{user_defined_column('ORGANIZATION_NOTES_UUID')},
        #{user_defined_column('ORGANIZATION_OCLCNO_UUID')},
        #{user_defined_column('ORGANIZATION_LIBRARIES_UUID')} 
      )
      SELECT #{env['PROJECT_MODEL_ID_ORGANIZATIONS'].to_i}, name, dates, archives, notes, oclc, libraries
        FROM #{table_name}
    SQL

    execute <<-SQL.squish
      WITH 
 
      all_people AS (

      SELECT editor as name, 'editor' AS type
        FROM #{table_name}
       WHERE editor IS NOT NULL
         AND TRIM(editor) != ''
       UNION
      SELECT publisher as name, 'publisher' AS type
        FROM #{table_name}
       WHERE publisher IS NOT NULL
         AND TRIM(publisher) != ''
       UNION
      SELECT contributors as name, 'contributors' AS type
        FROM #{table_name}
       WHERE contributors IS NOT NULL
         AND TRIM(contributors) != ''
          
      )

      INSERT INTO z_people ( project_model_id, name, last_name, first_name )
      SELECT #{env['PROJECT_MODEL_ID_PEOPLE'].to_i},
             name,
             regexp_replace(name, '^.*\\s+(\\S+)$', '\\1') AS last_name, 
             regexp_replace(name, '\\s+\\S+$', '') AS first_name
        FROM all_people
       GROUP BY all_people.name
      RETURNING id, name, uuid
    SQL

    execute <<-SQL.squish
      INSERT INTO z_places ( project_model_id, name, latitude, longitude )
      SELECT #{env['PROJECT_MODEL_ID_PLACES'].to_i}, address, latitude, longitude
        FROM #{table_name}
       WHERE address IS NOT NULL 
         AND TRIM(address) != ''
       GROUP BY address, latitude, longitude
    SQL

    execute <<-SQL
      INSERT INTO z_relationships ( 
        project_model_relationship_id, 
        primary_record_uuid, 
        primary_record_type, 
        related_record_uuid, 
        related_record_type 
      )
      SELECT #{env['PROJECT_MODEL_RELATIONSHIP_ID_EDITOR'].to_i}, 
             z_organizations.uuid, 
             'CoreDataConnector::Organization', 
             z_people.uuid, 
             'CoreDataConnector::Person'
        FROM #{table_name} z_temp
        JOIN z_organizations ON z_organizations.name = z_temp.name
        JOIN z_people ON z_people.name = z_temp.editor
       UNION
      SELECT #{env['PROJECT_MODEL_RELATIONSHIP_ID_PUBLISHER'].to_i}, 
             z_organizations.uuid, 
             'CoreDataConnector::Organization', 
             z_people.uuid, 
             'CoreDataConnector::Person'
        FROM #{table_name} z_temp
        JOIN z_organizations ON z_organizations.name = z_temp.name
        JOIN z_people ON z_people.name = z_temp.publisher
       UNION
      SELECT #{env['PROJECT_MODEL_RELATIONSHIP_ID_CONTRIBUTOR'].to_i}, 
             z_organizations.uuid, 
             'CoreDataConnector::Organization', 
             z_people.uuid, 
             'CoreDataConnector::Person'
        FROM #{table_name} z_temp
        JOIN z_organizations ON z_organizations.name = z_temp.name
        JOIN z_people ON z_people.name = z_temp.contributors
       UNION
      SELECT #{env['PROJECT_MODEL_RELATIONSHIP_ID_LOCATION'].to_i}, 
             z_organizations.uuid, 
             'CoreDataConnector::Organization', 
             z_places.uuid, 
             'CoreDataConnector::Place'
        FROM #{table_name} z_temp
        JOIN z_organizations ON z_organizations.name = z_temp.name
        JOIN z_places ON z_places.name = z_temp.address
    SQL
  end

  protected

  def column_names
    [{
       name: 'name',
       type: 'VARCHAR'
    }, {
       name: 'address',
       type: 'VARCHAR'
     }, {
       name: 'latitude',
       type: 'DECIMAL'
     }, {
       name: 'longitude',
       type: 'DECIMAL'
     }, {
       name: 'type',
       type: 'VARCHAR'
     }, {
       name: 'editor',
       type: 'VARCHAR'
     }, {
       name: 'publisher',
       type: 'VARCHAR'
     }, {
       name: 'contributors',
       type: 'VARCHAR'
     }, {
       name: 'printer',
       type: 'VARCHAR'
     }, {
       name: 'dates',
       type: 'VARCHAR'
     }, {
       name: 'archives',
       type: 'TEXT'
     }, {
       name: 'notes',
       type: 'TEXT'
     }, {
       name: 'oclc',
       type: 'VARCHAR'
     }, {
       name: 'libraries',
       type: 'VARCHAR'
     }]
  end
end

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-d DATABASE', '--database DATABASE', 'Database name'
  opts.on '-u USER', '--user USER', 'Database username'
  opts.on '-f FILE', '--file FILE', 'Source filepath'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
  opts.on '-e ENV', '--environment ENV', 'Environment'
end.parse!(into: options)

# Parse environment variables
env_manager = Env.new
env = env_manager.initialize_env('./scripts/gbof', options[:environment])

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
  "#{options[:output]}/organizations.csv",
  "#{options[:output]}/people.csv",
  "#{options[:output]}/places.csv",
  "#{options[:output]}/relationships.csv"
]

archive = Archive.new
archive.create_archive(filepaths, options[:output])