require 'active_support/core_ext/string/filters'
require 'json'
require 'optparse'

require_relative '../../core/csv/multi_adapter'
require_relative '../../core/archive'
require_relative '../../core/env'

class CsvImport < Csv::MultiAdapter
  attr_reader :columns

  def initialize(database:, user:, input:, output:, env:)
    super

    initialize_columns
  end

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

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_taxonomies
    SQL
  end

  def load
    super

    export 'organizations.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             name, 
             NULL AS description
        FROM z_organizations
       ORDER BY name
    SQL

    export 'people.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             last_name AS last_name, 
             first_name AS first_name, 
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
             longitude,
             #{user_defined_column('OEUVRE_TYPE_FIELD')},
             #{user_defined_column('OEUVRE_ETAT_FIELD')},
             #{user_defined_column('OEUVRE_NOTES_FIELD')},
             #{user_defined_column('OEUVRE_FICHE_CPRQ_FIELD')}
        FROM z_places
       ORDER BY name
    SQL

    export 'relationships.csv', <<-SQL.squish
      SELECT project_model_relationship_id, 
             NULL as uuid,
             primary_record_uuid, 
             primary_record_type, 
             related_record_uuid, 
             related_record_type
        FROM z_relationships
    SQL

    export 'taxonomies.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid,
             name
        FROM z_taxonomies
    SQL
  end

  def setup
    super

    execute <<-SQL.squish
      CREATE EXTENSION IF NOT EXISTS pgcrypto
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_organizations;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_organizations (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id INTEGER,
        project_model_id INTEGER,
        name VARCHAR,
        description VARCHAR
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_people;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_people (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id INTEGER,
        project_model_id INTEGER,
        name VARCHAR,
        last_name VARCHAR,
        first_name VARCHAR
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_places;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_places (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id INTEGER,
        airtable_src VARCHAR,
        project_model_id INTEGER,
        name VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
        #{user_defined_column('OEUVRE_TYPE_FIELD')} VARCHAR,
        #{user_defined_column('OEUVRE_ETAT_FIELD')} VARCHAR,
        #{user_defined_column('OEUVRE_NOTES_FIELD')} VARCHAR,
        #{user_defined_column('OEUVRE_FICHE_CPRQ_FIELD')} VARCHAR
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_relationships;
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

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_taxonomies;
    SQL

    execute <<-SQL
      CREATE TABLE z_taxonomies (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        project_model_id INTEGER,
        name VARCHAR
      )
    SQL
  end

  def transform
    super

    # Import lieu into z_places
    execute <<-SQL.squish
      INSERT INTO z_places (
        project_model_id,
        airtable_id,
        airtable_src,
        name
      )
      SELECT #{env['LIEU_MODEL'].to_i}, 
             record_id,
             'lieu',
             name
        FROM lieu
    SQL

    # Import lieu.mrc into taxonomies
    execute <<-SQL.squish
      INSERT INTO z_taxonomies ( project_model_id, name )
      SELECT #{env['MRC_MODEL'].to_i}, mrc
        FROM lieu
       GROUP BY mrc
    SQL

    # Import lieu -> MRC relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['LIEU_MRC_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_taxonomies.uuid,
             'CoreDataConnector::Taxonomy'
        FROM lieu
        JOIN z_places ON z_places.airtable_id = lieu.record_id
        JOIN z_taxonomies ON z_taxonomies.name = lieu.mrc
    SQL

    # Import lieu.region into taxonomies
    execute <<-SQL.squish
      INSERT INTO z_taxonomies ( project_model_id, name )
      SELECT #{env['REGION_ADMINISTRATIVE_MODEL'].to_i}, region
        FROM lieu
       GROUP BY region
    SQL

    # Import lieu -> region relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['LIEU_REGION_ADMINISTRATIVE_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_taxonomies.uuid,
             'CoreDataConnector::Taxonomy'
        FROM lieu
        JOIN z_places ON z_places.airtable_id = lieu.record_id
        JOIN z_taxonomies ON z_taxonomies.name = lieu.region
    SQL

    # Import individu into people
    execute <<-SQL.squish
      INSERT INTO z_people ( 
        project_model_id,
        airtable_id,
        last_name,
        first_name
      )
      SELECT #{env['INDIVIDU_MODEL'].to_i},
             record_id,
             SPLIT_PART(name, ',', 1),
             SPLIT_PART(name, ',', 2)
        FROM individu
    SQL

    # Import individu.occupations into taxonomies
    execute <<-SQL.squish
        INSERT INTO z_taxonomies ( project_model_id, name )
        SELECT #{env['OCCUPATION_MODEL'].to_i}, occupations
          FROM individu
         GROUP BY occupations
    SQL

    # Import individu -> occupations relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['INDIVIDU_OCCUPATIONS_RELATIONSHIP'].to_i},
             z_people.uuid,
             'CoreDataConnector::Person',
             z_taxonomies.uuid,
             'CoreDataConnector::Taxonomy'
        FROM individu
        JOIN z_people ON z_people.airtable_id = individu.record_id
        JOIN z_taxonomies ON z_taxonomies.name = individu.occupations
    SQL

    # Import oeuvre into z_places
    execute <<-SQL.squish
      INSERT INTO z_places (
        project_model_id,
        airtable_id,
        airtable_src,
        name,
        latitude,
        longitude,
        #{user_defined_column('OEUVRE_TYPE_FIELD')},
        #{user_defined_column('OEUVRE_ETAT_FIELD')},
        #{user_defined_column('OEUVRE_NOTES_FIELD')},
        #{user_defined_column('OEUVRE_FICHE_CPRQ_FIELD')}
      )
      SELECT #{env['OEUVRE_MODEL'].to_i},
             record_id,
             'oeuvre',
             name,
             latitude,
             longitude,
             type,
             etat,
             notes,
             fiche_cprq
        FROM oeuvre
    SQL

    # Import oeuvre -> lieu relationships
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['OEUVRE_LIEU_RELATIONSHIP'].to_i},
             z_places_oeuvre.uuid,
             'CoreDataConnector::Place',
             z_places_lieu.uuid,
             'CoreDataConnector::Place'
        FROM oeuvre
        JOIN z_places z_places_oeuvre ON z_places_oeuvre.airtable_id = oeuvre.record_id
                                     AND z_places_oeuvre.airtable_src = 'oeuvre'
        JOIN z_places z_places_lieu ON z_places_lieu.airtable_id = oeuvre.lieu_id
                                   AND z_places_lieu.airtable_src = 'lieu'
    SQL

    # Import oeuvre.organization into z_organizations
    execute <<-SQL.squish
        INSERT INTO z_organizations ( project_model_id, name )
        SELECT #{env['ORGANISATION_MODEL'].to_i}, organization
          FROM oeuvre
         GROUP BY organization
    SQL

    # Import oeuvre -> organization relationships
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['OEUVRE_ORGANISATION_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_organizations.uuid,
             'CoreDataConnector::Organization'
        FROM oeuvre
        JOIN z_places ON z_places.airtable_id = oeuvre.record_id
                     AND z_places.airtable_src = 'oeuvre'
        JOIN z_organizations ON z_organizations.name = oeuvre.organization
    SQL
  end

  protected

  def column_names(file_name)
    columns[file_name]
  end

  private

  def initialize_columns
    @columns = {}

    columns_content = File.read('./scripts/borgeau/columns.json')
    @columns = JSON.parse(columns_content)

    @columns.keys.each do |key|
      @columns[key] = @columns[key].map{ |i| i.transform_keys(&:to_sym) }
    end
  end
end

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-d DATABASE', '--database DATABASE', 'Database name'
  opts.on '-u USER', '--user USER', 'Database username'
  opts.on '-i INPUT', '--input INPUT', 'Input directory'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
  opts.on '-e ENV', '--environment ENV', 'Environment'
end.parse!(into: options)

# Parse environment variables
env_manager = Env.new
env = env_manager.initialize_env('./scripts/borgeau', options[:environment])

# Run the importer
import = CsvImport.new(
  database: options[:database],
  user: options[:user],
  input: options[:input],
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
  "#{options[:output]}/relationships.csv",
  "#{options[:output]}/taxonomies.csv"
]

archive = Archive.new
archive.create_archive(filepaths, options[:output])