require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

# The canonical way of computing the name of the env
# variable for a given field name so we don't have
# to type them all out over and over.
def get_udf_key(field_name)
  "UDF_#{field_name.upcase.gsub(/[^\w]/, '_')}"
end

# Parse environment variables
env = Dotenv.parse './scripts/gca/.env.development'

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

model_files = ['places']

column_names = [
  'Item Id',
  'Item URI',
  'Dublin Core:Description',
  'Dublin Core:Source',
  'Dublin Core:Publisher',
  'Dublin Core:Date',
  'Dublin Core:Contributor',
  'Dublin Core:Rights',
  'Dublin Core:Language',
  'Dublin Core:Type',
  'Dublin Core:Identifier',
  'Item Type Metadata:geolocation:address',
  'Item Type Metadata:County',
  'Item Type Metadata:Elev_f',
  'Item Type Metadata:Elev_m',
  'Item Type Metadata:TopoName',
  'Item Type Metadata:Coordinates',
  'Item Type Metadata:Identifier',
  'Item Type Metadata:URL',
  'tags'
]

fields = {
  places: {
    'name': 'Dublin Core:Title',
    'longitude': 'Item Type Metadata:geolocation:longitude',
    'latitude': 'Item Type Metadata:geolocation:latitude',
  }
}

# Fill the fields.places object with more
column_names.each do |col|
  fields[:places]["udf_#{env[get_udf_key(col)]}"] = col
end

# Run the importer
transform = Csv::PlainCsvIngester.new(
  input: options[:input],
  output: options[:output],
  env: env,
  fields: fields,
  model_files: model_files,
  id_column: 'Item Id'
)

transform.parse_models
transform.cleanup(['places'])

archive = Archive.new
archive.create_archive(["#{options[:output]}/places.csv"], options[:output])
