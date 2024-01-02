require 'active_support/core_ext/string/filters'
require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

EDITION_MODEL = 'CoreDataConnector::Item'

class CsvTransform
  def initialize(input:, output:, env:)
    @input_path = input
    @output_path = output
    @env = env

    @filenames = [
      'archives',
      'archives_places',
      'editions_archives',
      'editions_editions',
      'editions_people',
      'editions_publishers',
      'editions',
      'people',
      'places',
      'publishers_places',
      'publishers',
      'works'
    ]
  end

  def add_uuids
    @filenames.each do |filename|
      CSV.open("#{@output_path}/#{filename}.csv", 'w') do |csv_out|
        table = CSV.read("#{@input_path}/#{filename}.csv", headers: true)

        csv_out << [*table[0].to_h.keys, 'uuid']

        table.each do |row|
          row['uuid'] = SecureRandom.uuid
          csv_out << row
        end
      end
    end
  end

  # Set up the relationships file
  def init_relationships
    File.write(
      "#{@output_path}/relationships.csv",
      "project_model_relationship_id,primary_record_uuid,primary_record_type,related_record_uuid,related_record_type\n"
    )
  end

  def transform_relationship(relation_obj)
    [
      'project_model_relationship_id',
      'primary_record_uuid',
      'primary_record_type',
      'related_record_uuid',
      'related_record_type'
    ].map { |field| relation_obj[field] }
  end

  def parse_editions_archives
    primary_records = CSV.read("#{@output_path}/archives.csv", headers: true)
    related_records = CSV.read("#{@output_path}/editions.csv", headers: true)
    relations = CSV.read("#{@output_path}/editions_archives.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = related_records.find { |rr| rr['id'] == relation['editions_id'] }
        matching_related = primary_records.find { |pr| pr['id'] == relation['archives_id'] }

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_ARCHIVES'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = EDITION_MODEL
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Organization'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_editions_editions
    editions = CSV.read("#{@output_path}/editions.csv", headers: true)
    relations = CSV.read("#{@output_path}/editions_editions.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = editions.find { |ed| ed['id'] == relation['parent_edition_id']}
        matching_related = editions.find { |ed| ed['id'] == relation['child_edition_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_EDITIONS'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = EDITION_MODEL
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = EDITION_MODEL
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  # TODO: There are some user-defined fields on this relation.
  # The only UDF is "role", and all of these are either "author"
  # or null. Maybe we can just name the relationship "Authors" and
  # leave it at that.
  def parse_editions_people
    editions = CSV.read("#{@output_path}/editions.csv", headers: true)
    people = CSV.read("#{@output_path}/people.csv", headers: true)
    relations = CSV.read("#{@output_path}/editions_people.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = editions.find { |ed| ed['id'] == relation['editions_id']}
        matching_related = people.find { |ed| ed['id'] == relation['people_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_PEOPLE'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = EDITION_MODEL
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector:Person'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_editions_publishers
    editions = CSV.read("#{@output_path}/editions.csv", headers: true)
    publishers = CSV.read("#{@output_path}/publishers.csv", headers: true)
    relations = CSV.read("#{@output_path}/editions_publishers.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = editions.find { |ed| ed['id'] == relation['editions_id']}
        matching_related = publishers.find { |ed| ed['id'] == relation['publishers_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_PUBLISHERS'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = EDITION_MODEL
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector:Organization'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_publishers_places
    publishers = CSV.read("#{@output_path}/publishers.csv", headers: true)
    places = CSV.read("#{@output_path}/places.csv", headers: true)
    relations = CSV.read("#{@output_path}/publishers_places.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = publishers.find { |ed| ed['id'] == relation['publishers_id']}
        matching_related = places.find { |ed| ed['id'] == relation['places_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_PUBLISHERS'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector:Organization'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector:Place'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_archives_places
    archives = CSV.read("#{@output_path}/archives.csv", headers: true)
    places = CSV.read("#{@output_path}/places.csv", headers: true)
    relations = CSV.read("#{@output_path}/archives_places.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = archives.find { |ed| ed['id'] == relation['archives_id']}
        matching_related = places.find { |ed| ed['id'] == relation['places_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_ARCHIVES_PLACES'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector:Organization'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector:Place'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end
end

env = Dotenv.parse './scripts/rumpf/.env.development'

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-i INPUT', '--input INPUT', 'Input directory'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
end.parse!(into: options)

unless options[:input] && options[:output]
  puts 'Input and output directory paths are required in arguments.'
  exit 1
end

transform = CsvTransform.new(
  input: options[:input],
  output: options[:output],
  env: env
)

transform.init_relationships
transform.add_uuids
transform.parse_editions_archives
transform.parse_editions_editions
transform.parse_editions_people
transform.parse_editions_publishers
transform.parse_publishers_places
transform.parse_archives_places
