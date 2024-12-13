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
    people_events: {
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

  transform.parse_relation(
    primary_model: 'CoreDataConnector::Event',
    secondary_model: 'CoreDataConnector::Person',
    primary_csv: "#{output}/temp_events.csv",
    secondary_csv: "#{output}/temp_people.csv",
    primary_id_column: 'events_id',
    secondary_id_column: 'people_id',
    relation_csv: "#{input}/events_people.csv",
    project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_EVENTS_PEOPLE'],
    udfs: relation_udfs[:people_events]
  )

  transform.parse_relation(
    primary_model: 'CoreDataConnector::Person',
    secondary_model: 'CoreDataConnector::Place',
    primary_csv: "#{output}/temp_people.csv",
    secondary_csv: "#{output}/temp_places.csv",
    primary_id_column: 'people_id',
    secondary_id_column: 'places_id',
    relation_csv: "#{input}/people_places.csv",
    project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_PEOPLE_PLACES']
  )

  transform.parse_relation(
    primary_model: 'CoreDataConnector::Event',
    secondary_model: 'CoreDataConnector::Place',
    primary_csv: "#{output}/temp_events.csv",
    secondary_csv: "#{output}/temp_places.csv",
    primary_id_column: 'events_id',
    secondary_id_column: 'places_id',
    relation_csv: "#{input}/events_places.csv",
    project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_EVENTS_PLACES']
  )

  transform.parse_relation(
    primary_model: 'CoreDataConnector::Organization',
    secondary_model: 'CoreDataConnector::Person',
    primary_csv: "#{output}/temp_organizations.csv",
    secondary_csv: "#{output}/temp_people.csv",
    primary_id_column: 'organizations_id',
    secondary_id_column: 'people_id',
    relation_csv: "#{input}/organizations_people.csv",
    project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_ORGANIZATIONS_PEOPLE']
  )
  transform.parse_relation(
    primary_model: 'CoreDataConnector::Organization',
    secondary_model: 'CoreDataConnector::Place',
    primary_csv: "#{output}/temp_organizations.csv",
    secondary_csv: "#{output}/temp_places.csv",
    primary_id_column: 'organizations_id',
    secondary_id_column: 'places_id',
    relation_csv: "#{input}/organizations_places.csv",
    project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_ORGANIZATIONS_PLACES']
  )

  transform.cleanup(model_files)

  filepaths = [
    "#{output}/relationships.csv"
  ].concat(model_files.map { |file| "#{output}/#{file}.csv" })

  archive = Archive.new
  archive.create_archive(filepaths, output)
end
