require 'active_support/core_ext/string/filters'
require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'
require_relative '../../core/csv/directus_ingester'

class CsvTransform < Csv::DirectusIngester
  def parse_editions_editions
    editions = CSV.read("#{@output_path}/temp_items.csv", headers: true)
    relations = CSV.read("#{@input_path}/items_items.csv", headers: true)

    CSV.open("#{@output_path}/relationships.csv", 'a') do |csv_out|
      relations.each do |relation|
        matching_primary = editions.find { |ed| ed['directus_id'] == relation['parent_edition_id']}
        matching_related = editions.find { |ed| ed['directus_id'] == relation['child_edition_id']}

        if matching_related && matching_primary
          new_relation = {}
          new_relation['project_model_relationship_id'] = @env['PROJECT_MODEL_RELATIONSHIP_ID_ITEMS_ITEMS'].to_i
          new_relation['primary_record_uuid'] = matching_primary['uuid']
          new_relation['primary_record_type'] = 'CoreDataConnector::Item'
          new_relation['related_record_uuid'] = matching_related['uuid']
          new_relation['related_record_type'] = 'CoreDataConnector::Item'
          csv_out << order_relationship(new_relation)
        end
      end
    end
  end

  # The import service expects one file for each type
  # of model. Both archives and publishers are
  # organizations. Fortunately, they have the same list
  # of fields, so we can just concat the two files.
  def combine_organizations
    File.rename("#{@output_path}/temp_archives.csv", "#{@output_path}/temp_organizations.csv")

    CSV.open("#{@output_path}/temp_organizations.csv", 'a') do |csv_out|
      header_found = false
      CSV.foreach("#{@output_path}/temp_publishers.csv") do |pub|
        # Ignore the header row
        if header_found
          csv_out << pub
        end
        header_found = true
      end
    end

    File.delete("#{@output_path}/temp_publishers.csv")
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

model_files = [
  'archives',
  'items',
  'people',
  'places',
  'publishers',
  'works'
]

fields = {
  archives: {
    'name': 'name',
    'description': nil
  },
  items: {
    'name': 'title',
    "udf_#{env['UDF_EDITIONS_NOTES_UUID']}": 'notes',
    "udf_#{env['UDF_EDITIONS_TYPE_UUID']}": 'type',
    "udf_#{env['UDF_EDITIONS_FORMAT_UUID']}": 'format',
    "udf_#{env['UDF_EDITIONS_LINE_UUID']}": 'line',
    "udf_#{env['UDF_EDITIONS_BNF_UUID']}": 'bnf',
    "udf_#{env['UDF_EDITIONS_DPLA_UUID']}": 'dpla',
    "udf_#{env['UDF_EDITIONS_JISC_UUID']}": 'jisc',
    "udf_#{env['UDF_EDITIONS_PUBLICATION_DATE_UUID']}": 'publication_date'
  },
  people: {
    'last_name': nil,
    'first_name': 'full_name',
    'middle_name': nil,
    'biography': nil,
    "udf_#{env['UDF_PEOPLE_VIAF_UUID']}": 'viaf',
    "udf_#{env['UDF_PEOPLE_WIKIDATA_UUID']}": 'wikidata'
  },
  places: {
    'name': 'name',
    'latitude': nil,
    'longitude': nil,
    "udf_#{env['UDF_PLACES_VIAF_UUID']}": 'viaf',
    "udf_#{env['UDF_PLACES_WIKIDATA_UUID']}": 'wikidata'
  },
  publishers: {
    'name': 'name',
    'description': nil
  },
  works: {
    'name': 'name',
    "udf_#{env['UDF_WORKS_STATUS_UUID']}": 'status'
  }
}

transform = CsvTransform.new(
  input: options[:input],
  output: options[:output],
  env: env,
  fields: fields,
  model_files: model_files
)

transform.init_relationships
transform.parse_models
transform.parse_relation('items', 'archives', nil, 'CoreDataConnector::Organization')
transform.parse_editions_editions
transform.parse_relation('items', 'people')
transform.parse_relation('items', 'publishers', nil, 'CoreDataConnector::Organization')
transform.parse_relation('publishers', 'places', 'CoreDataConnector::Organization')
transform.parse_relation('archives', 'places', 'CoreDataConnector::Organization')
transform.combine_organizations
transform.remove_directus_ids([
  'items',
  'organizations',
  'people',
  'places',
  'works'
])

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
