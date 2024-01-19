require 'dotenv'
require 'optparse'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

env = Dotenv.parse './scripts/supplique/.env.development'

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
  'people',
  'places',
  'taxonomies',
  'works'
]

fields = {
  people: {
    'last_name': 'last_name',
    'first_name': 'first_name',
    'middle_name': nil,
    'biography': nil,
    "udf_#{env['UDF_PEOPLE_TITLE_UUID']}": 'title',
    "udf_#{env['UDF_PEOPLE_TYPE_UUID']}": 'type'
  },
  places: {
    'name': 'name',
    'latitude': nil,
    'longitude': nil,
    "udf_#{env['UDF_PLACES_POSTAL_CODE_UUID']}": 'postal_code',
  },
  taxonomies: {
    'word': 'word'
  },
  works: {
    'name': 'number_order',
    "udf_#{env['UDF_WORKS_NUMBER_ORDER_UUID']}": 'number_order',
    "udf_#{env['UDF_WORKS_YEAR_UUID']}": 'year',
    "udf_#{env['UDF_WORKS_MONTH_UUID']}": 'month',
    "udf_#{env['UDF_WORKS_DAY_UUID']}": 'day',
    "udf_#{env['UDF_WORKS_SUMMARY_UUID']}": 'summary',
    "udf_#{env['UDF_WORKS_REFERENCES_UUID']}": 'references',
    "udf_#{env['UDF_WORKS_COMMENTS_UUID']}": 'comments',
    "udf_#{env['UDF_WORKS_TEXT_UUID']}": 'texte',
  }
}

transform = Csv::PlainCsvIngester.new(
  input: options[:input],
  output: options[:output],
  env: env,
  fields: fields,
  model_files: model_files
)

transform.init_relationships
transform.parse_models

transform.parse_simple_relation('works', 'people')
transform.parse_simple_relation('works', 'places')
transform.parse_simple_relation('works', 'taxonomies')

transform.parse_relation(
  primary_model: 'CoreDataConnector::Work',
  secondary_model: 'CoreDataConnector::Person',
  primary_csv: "#{options[:output]}/temp_works.csv",
  secondary_csv: "#{options[:output]}/temp_people.csv",
  primary_id_column: 'works_id',
  secondary_id_column: 'people_id',
  relation_csv: "#{options[:input]}/works_commandements.csv",
  project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_WORKS_COMMANDEMENTS']
)

transform.parse_relation(
  primary_model: 'CoreDataConnector::Work',
  secondary_model: 'CoreDataConnector::Person',
  primary_csv: "#{options[:output]}/temp_works.csv",
  secondary_csv: "#{options[:output]}/temp_people.csv",
  primary_id_column: 'works_id',
  secondary_id_column: 'people_id',
  relation_csv: "#{options[:input]}/works_titulatures.csv",
  project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_WORKS_TITULATURES']
)

transform.parse_relation(
  primary_model: 'CoreDataConnector::Work',
  secondary_model: 'CoreDataConnector::Place',
  primary_csv: "#{options[:output]}/temp_works.csv",
  secondary_csv: "#{options[:output]}/temp_places.csv",
  primary_id_column: 'works_id',
  secondary_id_column: 'places_id',
  relation_csv: "#{options[:input]}/works_dates_of_place.csv",
  project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_WORKS_DATES_OF_PLACE']
)

transform.parse_relation(
  primary_model: 'CoreDataConnector::Work',
  secondary_model: 'CoreDataConnector::Place',
  primary_csv: "#{options[:output]}/temp_works.csv",
  secondary_csv: "#{options[:output]}/temp_places.csv",
  primary_id_column: 'works_id',
  secondary_id_column: 'places_id',
  relation_csv: "#{options[:input]}/works_villes.csv",
  project_model_relation_id: env['PROJECT_MODEL_RELATIONSHIP_ID_WORKS_VILLES']
)

transform.cleanup(model_files)

filepaths = [
  "#{options[:output]}/relationships.csv"
].concat(model_files.map { |file| "#{options[:output]}/#{file}.csv" })

archive = Archive.new
archive.create_archive(filepaths, options[:output])
