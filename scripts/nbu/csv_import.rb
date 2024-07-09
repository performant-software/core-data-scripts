require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'
require 'json'
require 'nokogiri'

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
    'events',
    'items',
    'people',
    'places',
    'taxonomies'
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

  def get_element_type(model_type)
    element_types = {
      'person': 'persName',
      'place': 'placeName',
      'event': 'div'
    }

    element_types[model_type.to_sym]
  end

  # Replace the old style of XML ID with a UUID in both the
  # XML resource and the Core Data relationship record.
  def migrate_xml_id(relation, item, related_record, model_type, env)
    new_xml_id = "_#{related_record['uuid']}"

    xml_path = File.expand_path("output/nbu/xml/#{item["udf_#{env['UDF_ITEMS_ARCHIVENGINE_ID_UUID'].gsub('-', '_')}"]}.xml")

    doc = File.open(xml_path) { |f| Nokogiri::XML(f) }

    # Update all the occurrences of this record in the text
    matches = doc.xpath("//xmlns:#{get_element_type(model_type)}[@sameAs=\"##{relation['xml_id']}\"]")
    matches.each do |el|
      el['sameAs'] = "##{new_xml_id}"
    end

    File.write(xml_path, doc)

    new_xml_id
  end

  def copy_input_xmls
    input_path = File.expand_path('input/nbu/xml')
    output_path = File.expand_path('output/nbu')

    FileUtils.cp_r(input_path, output_path)
  end

  # These will be generated fresh by the FCC Bridge the first time each document
  # is updated. I don't think we need to go through the trouble of migrating
  # the standoff XMLs to a new format when they're so temporary.
  def remove_standoffs
    path = File.expand_path('output/nbu/xml')
    files = Dir.entries(path)

    files.each do |file|
      unless ['.', '..'].include? file
        filepath = File.join(path, file)
        doc = File.open(filepath) { |f| Nokogiri::XML(f) }

        standoff = doc.at_xpath('//xmlns:standOff')

        if standoff
          standoff.remove
        end

        File.write(filepath, doc)
      end
    end
  end

  # There will still be some orphaned sameAs attributes in some docs,
  # where the corresponding relationship was deleted at some point
  # or (mostly) for merged documents. This function looks for sameAs
  # attributes that aren't UUIDs and removes them.
  def remove_dead_sameas_attributes
    path = File.expand_path('output/nbu/xml')
    files = Dir.entries(path)

    files.each do |file|
      unless ['.', '..'].include? file
        filepath = File.join(path, file)
        doc = File.open(filepath) { |f| Nokogiri::XML(f) }

        elements_with_sameas_atts = doc.xpath('//xmlns:*[@sameAs]')

        reg = /#[\p{L}]/

        elements_with_sameas_atts.each do |el|
          if reg.match(el['sameAs'])
            el.remove_attribute('sameAs')
          end
        end

        File.write(filepath, doc)
      end
    end
  end

  # Normalize the different possible values for enslavement status into
  # just two: `Enslaved` and `Enslaver`. They are not mutually exclusive.
  def transform_status(person)
    if !person['status']
      return nil
    end

    parsed = JSON.parse(person['status'])

    if !parsed.count || parsed.count == 0
      return nil
    end

    enslaved = false
    enslaver = false

    parsed.each do |str|
      if str.downcase.include?('enslaved')
        enslaved = true
      elsif str.downcase.include?('enslaver')
        enslaver = true
      end
    end

    result = []

    if enslaved
      result << 'Enslaved'
    end

    if enslaver
      result << 'Enslaver'
    end

    JSON.generate(result)
  end

  fields = {
    events: {
      'name': 'name',
      'description': 'description',
      'start_date': 'start_date',
      'start_date_description': 'date_description',
      'end_date': 'end_date',
      'end_date_description': nil,
      "udf_#{env['UDF_EVENTS_TYPE_UUID']}": 'type'
    },
    items: {
      'name': 'title',
      "udf_#{env['UDF_ITEMS_ARCHIVENGINE_ID_UUID']}": 'archivengine_id'
    },
    people: {
      'last_name': nil,
      'first_name': 'display_name',
      'middle_name': nil,
      'biography': nil,
      "udf_#{env['UDF_PEOPLE_GENDER_UUID']}": Proc.new { |person| transform_gender(person) },
      "udf_#{env['UDF_PEOPLE_BIRTHDATE_UUID']}": 'approximate_birth_year',
      "udf_#{env['UDF_PEOPLE_STATUS_UUID']}": Proc.new { |person| transform_status(person) },
      "udf_#{env['UDF_PEOPLE_OCCUPATION_UUID']}": 'occupation'
    },
    places: {
      'name': 'name',
      'latitude': nil,
      'longitude': nil
    },
    taxonomies: {
      'name': 'name'
    }
  }

  relation_udfs = {
    items_events: {
      "udf_#{env['UDF_DOCUMENTS_EVENTS_XML_ID_UUID']}": Proc.new { |relation, item, event| migrate_xml_id(relation, item, event, 'event', env) }
    },
    events_people: {
      "udf_#{env['UDF_EVENTS_PEOPLE_TYPE_UUID']}": 'type'
    },
    items_people: {
      "udf_#{env['UDF_ITEMS_PEOPLE_XML_ID_UUID']}": Proc.new { |relation, item, person| migrate_xml_id(relation, item, person, 'person', env) }
    },
    items_places: {
      "udf_#{env['UDF_ITEMS_PLACES_XML_ID_UUID']}": Proc.new { |relation, item, place| migrate_xml_id(relation, item, place, 'place', env) }
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

  transform.copy_input_xmls
  transform.remove_standoffs

  transform.parse_simple_relation('items', 'events')
  transform.parse_simple_relation('events', 'people')
  transform.parse_simple_relation('events', 'places')
  transform.parse_simple_relation('items', 'people')
  transform.parse_simple_relation('items', 'places')
  transform.parse_simple_relation('people', 'taxonomies')

  transform.parse_enslavements
  transform.parse_family_relations
  transform.parse_people_places

  transform.remove_dead_sameas_attributes

  transform.cleanup([
    'events',
    'items',
    'people',
    'places',
    'taxonomies'
  ])

  files = [
    "#{output}/events.csv",
    "#{output}/items.csv",
    "#{output}/people.csv",
    "#{output}/places.csv",
    "#{output}/relationships.csv",
    "#{output}/taxonomies.csv"
  ]

  archive = Archive.new
  archive.create_archive(files, output)
end
