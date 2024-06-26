require 'dotenv'
require 'optparse'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

def parse_bischoff
  input = File.expand_path('input/bischoff')
  output = File.expand_path('output/bischoff')

  env = Dotenv.parse './scripts/bischoff/.env.development'

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

  transform = Csv::PlainCsvIngester.new(
    input: input,
    output: output,
    id_map_path: File.expand_path('./id_maps/bischoff'),
    env: env,
    fields: fields,
    model_files: model_files,
    relation_udfs: relation_udfs
  )

  transform.parse_models
  transform.parse_simple_relation('items', 'places')
  transform.parse_simple_relation('items', 'works')
  transform.parse_simple_relation('items', 'organizations')
  transform.parse_simple_relation('organizations', 'places')
  transform.parse_simple_relation('works', 'people')
  transform.cleanup(model_files)

  filepaths = [
    "#{output}/relationships.csv"
  ].concat(model_files.map { |file| "#{output}/#{file}.csv" })

  archive = Archive.new
  archive.create_archive(filepaths, output)
end
