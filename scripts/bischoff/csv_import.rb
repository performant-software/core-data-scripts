require 'dotenv'
require 'optparse'

require_relative '../../core/archive'
require_relative '../../core/csv/directus_ingester'

class CsvTransform < Csv::DirectusIngester

end

env = Dotenv.parse './scripts/bischoff/.env.development'

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
  'items',
  'organizations',
  'people',
  'places',
  'works'
]

fields = {
  items: {
    'name': 'title',
    "udf_#{env['UDF_ITEMS_TEXT_UUID']}": 'text',
    "udf_#{env['UDF_ITEMS_MEASURES_UUID']}": 'measures',
    "udf_#{env['UDF_ITEMS_CATALOG_NUMBER_UUID']}": 'catalog_number',
    "udf_#{env['UDF_ITEMS_ORIG_DATE_UUID']}": 'orig_date',
    "udf_#{env['UDF_ITEMS_PROV_DATE_UUID']}": 'prov_date',
  },
  people: {
    'last_name': nil,
    'first_name': 'name',
    'middle_name': nil,
    'biography': nil,
  },
  places: {
    'name': 'name',
    'latitude': nil,
    'longitude': nil
  },
  organizations: {
    'name': 'name',
    'description': nil
  },
  works: {
    'name': 'title'
  }
}

relation_udfs = {
  items_places: {
    "udf_#{env['UDF_ITEMS_PLACES_TYPE_UUID']}": 'type',
    "udf_#{env['UDF_ITEMS_PLACES_SUBTYPE_UUID']}": 'subtype',
    "udf_#{env['UDF_ITEMS_PLACES_CERT_UUID']}": 'cert'
  },
  items_organizations: {
    "udf_#{env['UDF_ITEMS_ORGANIZATIONS_SHELFMARK_UUID']}": 'shelfmark',
    "udf_#{env['UDF_ITEMS_ORGANIZATIONS_SHELFMARK_SECTION_UUID']}": 'section',
    "udf_#{env['UDF_ITEMS_ORGANIZATIONS_FORMER_SHELFMARK_UUID']}": 'former_shelfmark',
  }
}

transform = CsvTransform.new(
  input: options[:input],
  output: options[:output],
  env: env,
  fields: fields,
  model_files: model_files,
  relation_udfs: relation_udfs
)

# TODO: don't forget about the UDFs in some of the relation files!!!!
transform.init_relationships
transform.parse_models
transform.parse_relation('items', 'places')
transform.parse_relation('items', 'works')
transform.parse_relation('items', 'organizations')
transform.parse_relation('organizations', 'places')
transform.parse_relation('works', 'people')
transform.remove_directus_ids(model_files)

filepaths = [
  "#{options[:output]}/relationships.csv"
].concat(model_files.map { |file| "#{options[:output]}/#{file}.csv" })

archive = Archive.new
archive.create_archive(filepaths, options[:output])
