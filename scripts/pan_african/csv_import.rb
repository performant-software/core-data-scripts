require 'dotenv'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

def parse_pan_african
  input = File.expand_path('input/pan_african')
  output = File.expand_path('output/pan_african')

  env = Dotenv.parse './scripts/pan_african/.env.production'

  model_files = [
    'events',
    'organizations',
    'people',
    'places'
  ]

  fields = {
    events: {
      'name': 'name',
      'description': nil,
      'start_date': nil,
      'start_date_description': nil,
      'end_date': nil,
      'end_date_description': nil,
      "udf_#{env['UDF_EVENTS_DATES_UUID']}": 'dates',
      "udf_#{env['UDF_EVENTS_NOTES_UUID']}": 'notes',
      "udf_#{env['UDF_EVENTS_EVENT_ID_UUID']}": 'id',
    },
    organizations: {
      'name': 'name',
      'description': nil,
      "udf_#{env['UDF_ORGANIZATIONS_NOTES_UUID']}": 'notes',
      "udf_#{env['UDF_ORGANIZATIONS_CITATION_UUID']}": 'citation',
      "udf_#{env['UDF_ORGANIZATIONS_ORGANIZATION_ID_UUID']}": 'id',
      "udf_#{env['UDF_ORGANIZATIONS_DATES_UUID']}": 'dates',
    },
    people: {
      'last_name': 'last_name',
      'first_name': 'first_name',
      'middle_name': nil,
      'biography': nil,
      "udf_#{env['UDF_PEOPLE_GENDER_CITATION_UUID']}": 'gender_citation',
      "udf_#{env['UDF_PEOPLE_OCCUPATION_CITATION_UUID']}": 'occupation_citation',
      "udf_#{env['UDF_PEOPLE_GENDER_UUID']}": 'gender',
      "udf_#{env['UDF_PEOPLE_OCCUPATION_UUID']}": 'occupation',
      "udf_#{env['UDF_PEOPLE_NOTES_UUID']}": 'notes',
    },
    places: {
      'name': 'name',
      'latitude': nil,
      'longitude': nil,
    }
  }

  relation_udfs = {
    events_people: {
      "udf_#{env['UDF_PEOPLE_EVENTS_CITATION_UUID']}": 'citation',
      "udf_#{env['UDF_PEOPLE_EVENTS_NOTES_UUID']}": 'notes'
    }
  }

  transform = Csv::PlainCsvIngester.new(
    input: input,
    output: output,
    id_map_path: File.expand_path('./id_maps/pan_african'),
    env: env,
    fields: fields,
    model_files: model_files,
    relation_udfs: relation_udfs
  )

  transform.parse_models

  transform.parse_simple_relation('events', 'people')
  transform.parse_simple_relation('people', 'places')
  transform.parse_simple_relation('events', 'places')
  transform.parse_simple_relation('organizations', 'people')
  transform.parse_simple_relation('organizations', 'places')

  transform.cleanup(model_files)

  filepaths = [
    "#{output}/relationships.csv"
  ].concat(model_files.map { |file| "#{output}/#{file}.csv" })

  archive = Archive.new
  archive.create_archive(filepaths, output)
end
