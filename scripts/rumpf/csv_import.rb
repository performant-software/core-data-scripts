require 'active_support/core_ext/string/filters'
require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'

EDITION_MODEL = 'CoreDataConnector::Item'

class CsvTransform
  def initialize(input:, output:, env:)
    @input_path = input
    @output_path = output
    @env = env

    @model_files = [
      'archives',
      'items',
      'people',
      'places',
      'publishers',
      'works'
    ]

    @relation_files = [
      'archives_places',
      'editions_archives',
      'editions_editions',
      'editions_people',
      'editions_publishers',
      'publishers_places'
    ]
  end

  def add_uuids
    @model_files.each do |filename|
      CSV.open("#{@output_path}/#{filename}.csv", 'w') do |csv_out|
        table = CSV.read("#{@input_path}/#{filename}.csv", headers: true)

        csv_out << [*table[0].to_h.keys, 'uuid', 'project_model_id']

        table.each do |row|
          row['uuid'] = SecureRandom.uuid
          row['project_model_id'] = @env["PROJECT_MODEL_ID_#{filename.upcase}"].to_i
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
    related_records = CSV.read("#{@output_path}/items.csv", headers: true)
    relations = CSV.read("#{@input_path}/editions_archives.csv", headers: true)

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
    editions = CSV.read("#{@output_path}/items.csv", headers: true)
    relations = CSV.read("#{@input_path}/editions_editions.csv", headers: true)

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
    editions = CSV.read("#{@output_path}/items.csv", headers: true)
    people = CSV.read("#{@output_path}/people.csv", headers: true)
    relations = CSV.read("#{@input_path}/editions_people.csv", headers: true)

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
          new_relation['related_record_type'] = 'CoreDataConnector::Person'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_editions_publishers
    editions = CSV.read("#{@output_path}/items.csv", headers: true)
    publishers = CSV.read("#{@output_path}/publishers.csv", headers: true)
    relations = CSV.read("#{@input_path}/editions_publishers.csv", headers: true)

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
          new_relation['related_record_type'] = 'CoreDataConnector::Organization'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_publishers_places
    publishers = CSV.read("#{@output_path}/publishers.csv", headers: true)
    places = CSV.read("#{@output_path}/places.csv", headers: true)
    relations = CSV.read("#{@input_path}/publishers_places.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = publishers.find { |ed| ed['id'] == relation['publishers_id']}
        matching_related = places.find { |ed| ed['id'] == relation['places_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_EDITIONS_PUBLISHERS'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Organization'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Place'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  def parse_archives_places
    archives = CSV.read("#{@output_path}/archives.csv", headers: true)
    places = CSV.read("#{@output_path}/places.csv", headers: true)
    relations = CSV.read("#{@input_path}/archives_places.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = archives.find { |ed| ed['id'] == relation['archives_id']}
        matching_related = places.find { |ed| ed['id'] == relation['places_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_ARCHIVES_PLACES'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Organization'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Place'
          csv_out << transform_relationship(new_relation)
        end
      end
    end
  end

  # The import service expects one file for each type
  # of model. Both archives and publishers are
  # organizations. Fortunately, they have the same list
  # of fields, so we can just concat the two files.
  def combine_organizations
    File.rename("#{@output_path}/archives.csv", "#{@output_path}/organizations.csv")

    CSV.open("#{@output_path}/organizations.csv", 'a') do |csv_out|
      header_found = false
      CSV.foreach("#{@output_path}/publishers.csv") do |pub|
        # Ignore the header row
        if header_found
          csv_out << pub
        end
        header_found = true
      end
    end

    File.delete("#{@output_path}/publishers.csv")
  end

  # Removes extraneous M2M files from the output dir.
  def cleanup
    @relation_files.each do |file|
      file_path = "#{@output_path}/#{file}.csv"
      File.delete(file_path) if File.exist?(file_path)
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
transform.combine_organizations
transform.cleanup

filepaths = [
  "#{options[:output]}/items.csv",
  "#{options[:output]}/organizations.csv",
  "#{options[:output]}/people.csv",
  "#{options[:output]}/places.csv",
  "#{options[:output]}/relationships.csv",
  "#{options[:output]}/works.csv"
]

archive = Archive.new
archive.create_archive(filepaths, options[:output])
