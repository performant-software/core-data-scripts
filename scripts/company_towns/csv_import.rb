require 'active_support/core_ext/string/filters'
require 'dotenv'
require 'json'
require 'optparse'

require_relative '../../core/csv/multi_adapter'
require_relative '../../core/archive'

class CsvImport < Csv::MultiAdapter
  attr_reader :columns

  def initialize(database:, user:, input:, output:, env:)
    super

    initialize_columns
  end

  def cleanup
    super

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_instances
    SQL

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

    export 'instances.csv', <<-SQL.squish
      SELECT project_model_id,
             uuid,
             name,
             #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_COMPAGNIE_FIELD')},
             #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_CREATEUR_FIELD')},
             #{user_defined_column('MEDIAGRAPHIE__REFERENCETITRE_FIELD')},
             #{user_defined_column('MEDIAGRAPHIE_ADRESSEDOCUMENT_FIELD')},
             #{user_defined_column('MEDIAGRAPHIE_NOTES_FIELD')},
             #{user_defined_column('MEDIAGRAPHIE_SERIAL_NUMBER_MEDIAGRAPH_FIELD')},
             #{user_defined_column('PATRIMOINE_DATE_DEBUT_FIELD')},
             #{user_defined_column('PATRIMOINE_DATEFIN_FIELD')},
             #{user_defined_column('PATRIMOINE_ENTITE_RESPONSABLE_FIELD')},
             #{user_defined_column('PATRIMOINE_NOTES_FIELD')},
             #{user_defined_column('PATRIMOINE_SERIAL_NUMBER_PATRI_FIELD')}
        FROM z_instances
       ORDER BY name
    SQL

    export 'organizations.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             name, 
             NULL AS description,
             #{user_defined_column('COMPAGNIES__ARCHIVES_FIELD')},
             #{user_defined_column('COMPAGNIES_ANNEE_CREATION_FIELD')},
             #{user_defined_column('COMPAGNIES_ANNEE_DISSOLUTION_FIELD')},
             #{user_defined_column('COMPAGNIES_AUTRE_NOM_COMPAGNIE_FIELD')},
             #{user_defined_column('COMPAGNIES_INDUSTRIE_TYPE_FIELD')},
             #{user_defined_column('COMPAGNIES_NOTES_FIELD')},
             #{user_defined_column('COMPAGNIES_PAGES_WEB_FIELD')},
             #{user_defined_column('COMPAGNIES_SERIAL_NUMBER_COMPAGNIES_FIELD')},
             #{user_defined_column('FONDS_ARCHIVES_DEPOT_FIELD')},
             #{user_defined_column('FONDS_ARCHIVES_NOTES_FIELD')},
             #{user_defined_column('FONDS_ARCHIVES_OUTIL_DE_RECHERCHE_ADRESSE_FIELD')},
             #{user_defined_column('FONDS_ARCHIVES_SERIAL_NUMBER_ARCHIVES_FIELD')}
        FROM z_organizations
       ORDER BY name
    SQL

    export 'people.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             NULL AS last_name, 
             name AS first_name, 
             NULL AS middle_name, 
             NULL AS biography,
             #{user_defined_column('CREATEURS_DATE_DECES_FIELD')},
             #{user_defined_column('CREATEURS_DATE_NAISSANCE_FIELD')},
             #{user_defined_column('CREATEURS_NOTES_FIELD')},
             #{user_defined_column('CREATEURS_NOTICE_BIO_FIELD')},
             #{user_defined_column('CREATEURS_PAGES_WEB_FIELD')},
             #{user_defined_column('CREATEURS_SERIAL_NUMBER_CREA_FIELD')}
        FROM z_people
       ORDER BY last_name, first_name
    SQL

    export 'places.csv', <<-SQL.squish
      SELECT project_model_id, 
             uuid, 
             name, 
             latitude, 
             longitude,
             #{user_defined_column('VILLES_PERIODE_PLANIFICATION_FIELD')},
             #{user_defined_column('VILLES__NOVILLEMYSQL_FIELD')},
             #{user_defined_column('VILLES_ANNEE_FERMETURE_FIELD')},
             #{user_defined_column('VILLES_ANNEE_PLANIFICATION_FIELD')},
             #{user_defined_column('VILLES_ANNEE_PLANIFICATION_CIRCA_FIELD')},
             #{user_defined_column('VILLES_AUTRE_NOM_OU_LOCALISATION_FIELD')},
             #{user_defined_column('VILLES_AUTRES_INFORMATIONS_1_FIELD')},
             #{user_defined_column('VILLES_AUTRES_INFORMATIONS_2_FIELD')},
             #{user_defined_column('VILLES_NOTES_DE_RECHERCHE_FIELD')},
             #{user_defined_column('VILLES_SERIAL_NUMBER_FIELD')},
             #{user_defined_column('VILLES_FICHE_SYNTHESE_ADRESSE_FIELD')},
             #{user_defined_column('VILLES_TEXTE_INTERPRETATIF_FIELD')},
             #{user_defined_column('VILLES_URBANISME_FIELD')},
             #{user_defined_column('VILLES_VERIFICATION_FIELD')}
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
      DROP TABLE IF EXISTS z_instances;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_instances (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id VARCHAR,
        project_model_id INTEGER,
        name VARCHAR,
        #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_COMPAGNIE_FIELD')} VARCHAR,
        #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_CREATEUR_FIELD')} VARCHAR,
        #{user_defined_column('MEDIAGRAPHIE__REFERENCETITRE_FIELD')} VARCHAR,
        #{user_defined_column('MEDIAGRAPHIE_ADRESSEDOCUMENT_FIELD')} VARCHAR,
        #{user_defined_column('MEDIAGRAPHIE_NOTES_FIELD')} TEXT,
        #{user_defined_column('MEDIAGRAPHIE_SERIAL_NUMBER_MEDIAGRAPH_FIELD')} INTEGER,
        #{user_defined_column('PATRIMOINE_DATE_DEBUT_FIELD')} INTEGER,
        #{user_defined_column('PATRIMOINE_DATEFIN_FIELD')} INTEGER,
        #{user_defined_column('PATRIMOINE_ENTITE_RESPONSABLE_FIELD')} VARCHAR,
        #{user_defined_column('PATRIMOINE_NOTES_FIELD')} TEXT,
        #{user_defined_column('PATRIMOINE_SERIAL_NUMBER_PATRI_FIELD')} INTEGER
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_organizations;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_organizations (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id VARCHAR,
        project_model_id INTEGER,
        name VARCHAR,
        description VARCHAR,
        #{user_defined_column('COMPAGNIES__ARCHIVES_FIELD')} VARCHAR,
        #{user_defined_column('COMPAGNIES_ANNEE_CREATION_FIELD')} INTEGER,
        #{user_defined_column('COMPAGNIES_ANNEE_DISSOLUTION_FIELD')} INTEGER,
        #{user_defined_column('COMPAGNIES_AUTRE_NOM_COMPAGNIE_FIELD')} VARCHAR,
        #{user_defined_column('COMPAGNIES_INDUSTRIE_TYPE_FIELD')} VARCHAR,
        #{user_defined_column('COMPAGNIES_NOTES_FIELD')} TEXT,
        #{user_defined_column('COMPAGNIES_PAGES_WEB_FIELD')} VARCHAR,
        #{user_defined_column('COMPAGNIES_SERIAL_NUMBER_COMPAGNIES_FIELD')} INTEGER,
        #{user_defined_column('FONDS_ARCHIVES_DEPOT_FIELD')} VARCHAR,
        #{user_defined_column('FONDS_ARCHIVES_NOTES_FIELD')} TEXT,
        #{user_defined_column('FONDS_ARCHIVES_OUTIL_DE_RECHERCHE_ADRESSE_FIELD')} TEXT,
        #{user_defined_column('FONDS_ARCHIVES_SERIAL_NUMBER_ARCHIVES_FIELD')} INTEGER
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_people;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_people (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id VARCHAR,
        project_model_id INTEGER,
        name VARCHAR,
        last_name VARCHAR,
        first_name VARCHAR,
        #{user_defined_column('CREATEURS_DATE_DECES_FIELD')} INTEGER,
        #{user_defined_column('CREATEURS_DATE_NAISSANCE_FIELD')} INTEGER,
        #{user_defined_column('CREATEURS_NOTES_FIELD')} TEXT,
        #{user_defined_column('CREATEURS_NOTICE_BIO_FIELD')} TEXT,
        #{user_defined_column('CREATEURS_PAGES_WEB_FIELD')} VARCHAR,
        #{user_defined_column('CREATEURS_SERIAL_NUMBER_CREA_FIELD')} INTEGER
      )
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_places;
    SQL

    execute <<-SQL.squish
      CREATE TABLE z_places (
        id SERIAL,
        uuid UUID DEFAULT gen_random_uuid(),
        airtable_id VARCHAR,
        project_model_id INTEGER,
        name VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
        #{user_defined_column('VILLES_PERIODE_PLANIFICATION_FIELD')} VARCHAR,
        #{user_defined_column('VILLES__NOVILLEMYSQL_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_ANNEE_FERMETURE_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_ANNEE_PLANIFICATION_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_ANNEE_PLANIFICATION_CIRCA_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_AUTRE_NOM_OU_LOCALISATION_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_AUTRES_INFORMATIONS_1_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_AUTRES_INFORMATIONS_2_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_NOTES_DE_RECHERCHE_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_SERIAL_NUMBER_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_FICHE_SYNTHESE_ADRESSE_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_TEXTE_INTERPRETATIF_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_URBANISME_FIELD')} VARCHAR,
        #{user_defined_column('VILLES_VERIFICATION_FIELD')} VARCHAR
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

    # Import villes into z_places
    execute <<-SQL.squish
      INSERT INTO z_places (
        project_model_id,
        airtable_id,
        name,
        latitude,
        longitude,
        #{user_defined_column('VILLES_PERIODE_PLANIFICATION_FIELD')},
        #{user_defined_column('VILLES__NOVILLEMYSQL_FIELD')},
        #{user_defined_column('VILLES_ANNEE_FERMETURE_FIELD')},
        #{user_defined_column('VILLES_ANNEE_PLANIFICATION_FIELD')},
        #{user_defined_column('VILLES_ANNEE_PLANIFICATION_CIRCA_FIELD')},
        #{user_defined_column('VILLES_AUTRE_NOM_OU_LOCALISATION_FIELD')},
        #{user_defined_column('VILLES_AUTRES_INFORMATIONS_1_FIELD')},
        #{user_defined_column('VILLES_AUTRES_INFORMATIONS_2_FIELD')},
        #{user_defined_column('VILLES_NOTES_DE_RECHERCHE_FIELD')},
        #{user_defined_column('VILLES_SERIAL_NUMBER_FIELD')},
        #{user_defined_column('VILLES_FICHE_SYNTHESE_ADRESSE_FIELD')},
        #{user_defined_column('VILLES_TEXTE_INTERPRETATIF_FIELD')},
        #{user_defined_column('VILLES_URBANISME_FIELD')},
        #{user_defined_column('VILLES_VERIFICATION_FIELD')}
      )
      SELECT #{env['VILLES_MODEL'].to_i}, 
             record_id,
             nomvilledeciekey,
             latitude, 
             longitude,
             periodeplanification,
             _novillemysql,
             anneefermeture,
             anneeplanification,
             anneeplanificationcirca,
             autrenomoulocalisation,
             autres_informations_1,
             autres_informations_2,
             notesderecherche,
             serialnumber,
             fichesyntheseadresse,
             texte_interpretatif,
             urbanisme,
             verification
        FROM villes
    SQL

    # Import compagnies into z_organizations
    execute <<-SQL.squish
      INSERT INTO z_organizations (
        project_model_id,
        airtable_id,
        name,
        description,
        #{user_defined_column('COMPAGNIES__ARCHIVES_FIELD')},
        #{user_defined_column('COMPAGNIES_ANNEE_CREATION_FIELD')},
        #{user_defined_column('COMPAGNIES_ANNEE_DISSOLUTION_FIELD')},
        #{user_defined_column('COMPAGNIES_AUTRE_NOM_COMPAGNIE_FIELD')},
        #{user_defined_column('COMPAGNIES_INDUSTRIE_TYPE_FIELD')},
        #{user_defined_column('COMPAGNIES_NOTES_FIELD')},
        #{user_defined_column('COMPAGNIES_PAGES_WEB_FIELD')},
        #{user_defined_column('COMPAGNIES_SERIAL_NUMBER_COMPAGNIES_FIELD')}
      )
      SELECT #{env['COMPAGNIES_MODEL'].to_i}, 
             record_id,
             nomcompagniekey,
             NULL,
             _archives,
             anneecreation,
             anneedissolution,
             autrenomcompagnie,
             industrietype,
             notes,
             pagesweb,
             serialnumbercompagnies
        FROM compagnies
    SQL

    # Import fonds_archives into z_organizations
    execute <<-SQL.squish
      INSERT INTO z_organizations (
        project_model_id,
        airtable_id,
        name,
        description,
        #{user_defined_column('FONDS_ARCHIVES_DEPOT_FIELD')},
        #{user_defined_column('FONDS_ARCHIVES_NOTES_FIELD')},
        #{user_defined_column('FONDS_ARCHIVES_OUTIL_DE_RECHERCHE_ADRESSE_FIELD')},
        #{user_defined_column('FONDS_ARCHIVES_SERIAL_NUMBER_ARCHIVES_FIELD')}
      )
      SELECT #{env['FONDS_ARCHIVES_MODEL'].to_i},
             record_id,
             fondsetreferencekey, 
             NULL,
             depot,
             notes,
             outilderechercheadresse,
             serialnumberarchives
        FROM fonds_archives
    SQL

    # Import creatures into z_people
    execute <<-SQL.squish
      INSERT INTO z_people (
        project_model_id,
        airtable_id,
        name,
        #{user_defined_column('CREATEURS_DATE_DECES_FIELD')},
        #{user_defined_column('CREATEURS_DATE_NAISSANCE_FIELD')},
        #{user_defined_column('CREATEURS_NOTES_FIELD')},
        #{user_defined_column('CREATEURS_NOTICE_BIO_FIELD')},
        #{user_defined_column('CREATEURS_PAGES_WEB_FIELD')},
        #{user_defined_column('CREATEURS_SERIAL_NUMBER_CREA_FIELD')}
      )
      SELECT #{env['CREATEURS_MODEL'].to_i},
             record_id,
             nomcreateurkey, 
             datedeces,
             datenaissance,
             notes,
             noticebio,
             pagesweb,
             serialnumbercrea
        FROM createurs
    SQL

    # Import mediagraphie into z_instances
    execute <<-SQL.squish
      INSERT INTO z_instances (
        project_model_id,
        airtable_id,
        name,
        #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_COMPAGNIE_FIELD')},
        #{user_defined_column('MEDIAGRAPHIE__AUTRESUJET_CREATEUR_FIELD')},
        #{user_defined_column('MEDIAGRAPHIE__REFERENCETITRE_FIELD')},
        #{user_defined_column('MEDIAGRAPHIE_ADRESSEDOCUMENT_FIELD')},
        #{user_defined_column('MEDIAGRAPHIE_NOTES_FIELD')},
        #{user_defined_column('MEDIAGRAPHIE_SERIAL_NUMBER_MEDIAGRAPH_FIELD')}
      )
      SELECT #{env['MEDIAGRAPHIE_MODEL'].to_i},
             record_id,
             referencemediakey,
             _autresujet_compagnie,
             _autresujet_createur,
             _referencetitre,
             adressedocument,
             notes,
             serialnumbermediagraph
        FROM mediagraphie
    SQL

    # Import patrimoine into z_instances
    execute <<-SQL.squish
      INSERT INTO z_instances (
        project_model_id,
        airtable_id,
        name,
        #{user_defined_column('PATRIMOINE_DATE_DEBUT_FIELD')},
        #{user_defined_column('PATRIMOINE_DATEFIN_FIELD')},
        #{user_defined_column('PATRIMOINE_ENTITE_RESPONSABLE_FIELD')},
        #{user_defined_column('PATRIMOINE_NOTES_FIELD')},
        #{user_defined_column('PATRIMOINE_SERIAL_NUMBER_PATRI_FIELD')}
      )
      SELECT #{env['PATRIMOINE_MODEL'].to_i},
             record_id,
             referencetitrekey,
             datedebut,
             datefin,
             entiteresponsable,
             notes,
             serialnumberpatri
        FROM patrimoine
    SQL

    # Import villes.pays into z_taxonomies
    execute <<-SQL.squish
      INSERT INTO z_taxonomies (
        project_model_id,
        name
      )
      SELECT #{env['PAYS_MODEL'].to_i},
             pays
        FROM villes
       GROUP BY pays
       UNION
      SELECT #{env['PAYS_MODEL'].to_i},
             pays
        FROM compagnies
       GROUP BY pays
    SQL

    # Patrimoines -> Creatures relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['PATRIMOINE_CREATEUR_RELATIONSHIP'].to_i},
             z_instances.uuid,
             'CoreDataConnector::Instance',
             z_people.uuid,
             'CoreDataConnector::Person'
        FROM patrimoine
        JOIN z_instances ON z_instances.airtable_id = patrimoine.record_id
        JOIN z_people ON z_people.airtable_id = patrimoine._createurs_record_id
    SQL

    # Compagnies -> Pays relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['COMPAGNIES_PAYS_RELATIONSHIP'].to_i},
             z_organizations.uuid,
             'CoreDataConnector::Organization',
             z_taxonomies.uuid,
             'CoreDataConnector::Taxonomy'
        FROM compagnies
        JOIN z_organizations ON z_organizations.airtable_id = compagnies.record_id
        JOIN z_taxonomies ON z_taxonomies.name = compagnies.pays
       WHERE compagnies.pays IS NOT NULL
         AND compagnies.pays != ''
    SQL

    # Createurs -> Villes relationship (Lieu Deces)
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['CREATEURS_LIEU_DECES_RELATIONSHIP'].to_i},
             z_people.uuid,
             'CoreDataConnector::Person',
             z_places.uuid,
             'CoreDataConnector::Place'
        FROM createurs
        JOIN z_people ON z_people.airtable_id = createurs.record_id
        JOIN z_places ON z_places.airtable_id = createurs.lieudeces_record_id
    SQL

    # Createurs -> Villes relationship (Lieu Naissance)
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['CREATEURS_LIEU_NAISSANCE_RELATIONSHIP'].to_i},
             z_people.uuid,
             'CoreDataConnector::Person',
             z_places.uuid,
             'CoreDataConnector::Place'
        FROM createurs
        JOIN z_people ON z_people.airtable_id = createurs.record_id
        JOIN z_places ON z_places.airtable_id = createurs.lieunaissance_record_id
    SQL

    # Createurs -> Villes relationship (Ville)
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['CREATEURS_VILLE_RELATIONSHIP'].to_i},
             z_people.uuid,
             'CoreDataConnector::Person',
             z_places.uuid,
             'CoreDataConnector::Place'
        FROM createurs
        JOIN z_people ON z_people.airtable_id = createurs.record_id
        JOIN z_places ON z_places.airtable_id = createurs._villesdecie_record_id
    SQL

    # Villes -> Compagnies (Compagnies)
    execute <<-SQL.squish
      WITH

      unnested_villes AS (
        SELECT villes.record_id, UNNEST(string_to_array(villes.compagnies_record_id, ', ')) AS compagnies_record_id
          FROM villes
         WHERE villes.compagnies_record_id IS NOT NULL
           AND villes.compagnies_record_id != ''
      )

      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['VILLES_COMPAGNIES_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_organizations.uuid,
             'CoreDataConnector::Organization'
        FROM unnested_villes villes
        JOIN z_places ON z_places.airtable_id = villes.record_id
        JOIN z_organizations ON z_organizations.airtable_id = villes.compagnies_record_id
    SQL

    # Villes -> Fonds Archives (Fonds Archives)
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['VILLES_FONDS_ARCHIVES_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_organizations.uuid,
             'CoreDataConnector::Organization'
        FROM fonds_archives
        JOIN z_organizations ON z_organizations.airtable_id = fonds_archives.record_id
        JOIN z_places ON z_places.airtable_id = fonds_archives._nomvilledecompagnie_record_id
    SQL

    # Villes -> Mediagraphie (Mediagraphie)
    execute <<-SQL.squish
      WITH

      unnested_villes AS (
        SELECT villes.record_id, UNNEST(string_to_array(villes.mediagraphie_record_id, ', ')) AS mediagraphie_record_id
          FROM villes
         WHERE villes.mediagraphie_record_id IS NOT NULL
           AND villes.mediagraphie_record_id != ''
      )

      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['VILLES_MEDIAGRAPHIE_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_instances.uuid,
             'CoreDataConnector::Instance'
        FROM unnested_villes villes
        JOIN z_places ON z_places.airtable_id = villes.record_id
        JOIN z_instances ON z_instances.airtable_id = villes.mediagraphie_record_id
    SQL

    # Villes -> Patrimoine (Patrimoine)
    execute <<-SQL.squish
      WITH

      unnested_villes AS (
        SELECT villes.record_id, UNNEST(string_to_array(villes.patrimoine_record_id, ', ')) AS patrimoine_record_id
          FROM villes
         WHERE villes.patrimoine_record_id IS NOT NULL
           AND villes.patrimoine_record_id != ''
      )

      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['VILLES_PATRIMOINE_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_instances.uuid,
             'CoreDataConnector::Instance'
        FROM unnested_villes villes
        JOIN z_places ON z_places.airtable_id = villes.record_id
        JOIN z_instances ON z_instances.airtable_id = villes.patrimoine_record_id
    SQL

    # Villes -> Pays relationship
    execute <<-SQL.squish
      INSERT INTO z_relationships (
        project_model_relationship_id,
        primary_record_uuid,
        primary_record_type,
        related_record_uuid,
        related_record_type
      )
      SELECT #{env['VILLES_PAYS_RELATIONSHIP'].to_i},
             z_places.uuid,
             'CoreDataConnector::Place',
             z_taxonomies.uuid,
             'CoreDataConnector::Taxonomy'
        FROM villes
        JOIN z_places ON z_places.airtable_id = villes.record_id
        JOIN z_taxonomies ON z_taxonomies.name = villes.pays
       WHERE villes.pays IS NOT NULL
         AND villes.pays != ''
    SQL
  end

  protected

  def column_names(file_name)
    columns[file_name]
  end

  private

  def initialize_columns
    @columns = {}

    columns_content = File.read('./scripts/company_towns/columns.json')
    @columns = JSON.parse(columns_content)

    @columns.keys.each do |key|
      @columns[key] = @columns[key].map{ |i| i.transform_keys(&:to_sym) }
    end
  end
end

# Parse environment variables
env = Dotenv.parse './scripts/company_towns/.env.development'

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-d DATABASE', '--database DATABASE', 'Database name'
  opts.on '-u USER', '--user USER', 'Database username'
  opts.on '-i INPUT', '--input INPUT', 'Input directory'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
end.parse!(into: options)

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
  "#{options[:output]}/instances.csv",
  "#{options[:output]}/organizations.csv",
  "#{options[:output]}/people.csv",
  "#{options[:output]}/places.csv",
  "#{options[:output]}/relationships.csv",
  "#{options[:output]}/taxonomies.csv"
]

archive = Archive.new
archive.create_archive(filepaths, options[:output])