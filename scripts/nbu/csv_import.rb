require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

class NbuTransform < Csv::PlainCsvIngester
  def parse_family_relations
    people = CSV.read("#{@output_path}/temp_people.csv", headers: true)
    relations_table = CSV.read("#{@input_path}/family_relations.csv", headers: true)

    relation_types = {
      parent: @env['PROJECT_MODEL_FAMILY_RELATION_ID_PARENT_CHILD'],
      'adoptive parent': @env['PROJECT_MODEL_FAMILY_RELATION_ID_ADOPTIVE_PARENT_ADOPTIVE_CHILD'],
      grandparent: @env['PROJECT_MODEL_FAMILY_RELATION_ID_GRANDPARENT_GRANDCHILD'],
      'great grandparent': @env['PROJECT_MODEL_FAMILY_RELATION_ID_GREAT_GRANDPARENT_GREAT_GRANDCHILD'],
      godparent: @env['PROJECT_MODEL_FAMILY_RELATION_ID_GODPARENT_GODCHILD'],
      spouse: @env['PROJECT_MODEL_FAMILY_RELATION_ID_SPOUSE'],
      'wedding godparent': @env['PROJECT_MODEL_FAMILY_RELATION_ID_WEDDING_GODPARENT_WEDDING_GODCHILD']
    }

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations_table.each do |relation|
        relation_type_id = relation_types[relation['primary_name_neutral'].to_sym]

        unless relation_type_id
          puts "Relation type \"#{relation['primary_name_neutral']}/#{relation['secondary_name_neutral']}\" from family_relations not found in env file!"
          next
        end

        matching_primary = people.find { |p| p['original_id'] == relation['primary_person_id']}
        matching_related = people.find { |p| p['original_id'] == relation['secondary_person_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = relation_type_id.to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Person'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Person'

          csv_out << order_relationship(new_relation)
        end
      end
    end
  end

  def parse_enslavements
    people = CSV.read("#{@output_path}/temp_people.csv", headers: true)
    enslavements = CSV.read("#{@input_path}/enslavements.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      enslavements.each do |enslavement|
        matching_enslaver = people.find { |p| p['original_id'] == enslavement['enslaver_id'] }
        matching_enslaved = people.find { |p| p['original_id'] == enslavement['enslaved_id'] }
        
        if matching_enslaver && matching_enslaved
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ENSLAVEMENTS'].to_i
          new_relation['primary_record_uuid'] = matching_enslaver['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Person'
          new_relation['related_record_uuid'] = matching_enslaved['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Person'

          csv_out << order_relationship(new_relation)
        end
      end
    end
  end

  def parse_people_places
    people_table = CSV.read("#{@output_path}/temp_people.csv", headers: true)
    places_table = CSV.read("#{@output_path}/temp_places.csv", headers: true)
    relations_table = CSV.read("#{@input_path}/people_places.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations_table.each do |relation|
        relation_type_id = @env["PROJECT_MODEL_PEOPLE_PLACES_#{relation['type'].gsub(' ', '_').upcase}_ID"]

        unless relation_type_id
          puts "Relation type \"#{relation['type']}\" from people_places not found in env file!"
          next
        end

        matching_primary = people_table.find { |p| p['original_id'] == relation['people_id']}
        matching_related = places_table.find { |p| p['original_id'] == relation['places_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = relation_type_id.to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Person'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Place'

          csv_out << order_relationship(new_relation)
        end
      end
    end
  end
end

def parse_nbu
  input = File.expand_path('input/nbu')
  output = File.expand_path('output/nbu')

  # Parse environment variables
  env = Dotenv.parse './scripts/nbu/.env.development'

  model_files = [
    'people',
    'places',
    'taxonomies',
    'works'
  ]

  # In the NBU CMS, gender is an enum, so its values are 0 or 1.
  # In Core Data, it's a user-defined Select field with options
  # for "Male" and "Female", so we need to transform this column.
  def transform_gender(person)
    if person['gender'] == '0'
      person['gender'] = 'Male'
    elsif person['gender'] == '1'
      person['gender'] = 'Female'
    end
  end

  fields = {
    people: {
      'last_name': nil,
      'first_name': 'display_name',
      'middle_name': nil,
      'biography': nil,
      "udf_#{env['UDF_PEOPLE_GENDER_UUID']}": Proc.new { |person| transform_gender(person) }
    },
    places: {
      'name': 'name',
      'latitude': nil,
      'longitude': nil
    },
    taxonomies: {
      'name': 'name'
    },
    works: {
      'name': 'title',
      "udf_#{env['UDF_WORKS_ARCHIVENGINE_ID_UUID']}": 'archivengine_id'
    },
  }

  relation_udfs = {
    works_people: {
      "udf_#{env['UDF_WORKS_PEOPLE_XML_ID_UUID']}": 'xml_id'
    },
    works_places: {
      "udf_#{env['UDF_WORKS_PLACES_XML_ID_UUID']}": 'xml_id'
    }
  }

  # Run the importer
  transform = NbuTransform.new(
    input: input,
    output: output,
    id_map_path: File.expand_path('./id_maps/nbu'),
    env: env,
    fields: fields,
    model_files: model_files,
    relation_udfs: relation_udfs,
    id_map_column: 'slug'
  )

  transform.parse_models

  transform.parse_simple_relation('people', 'taxonomies')
  transform.parse_simple_relation('works', 'people')
  transform.parse_simple_relation('works', 'places')

  transform.parse_enslavements
  transform.parse_family_relations
  transform.parse_people_places

  transform.cleanup([
    'people',
    'places',
    'taxonomies',
    'works'
  ])

  files = [
    "#{output}/people.csv",
    "#{output}/places.csv",
    "#{output}/relationships.csv",
    "#{output}/taxonomies.csv",
    "#{output}/works.csv"
  ]

  archive = Archive.new
  archive.create_archive(files, output)
end
