require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

class CsvTransform < Csv::PlainCsvIngester
  def populate_taxonomies
    places = CSV.read("#{@output_path}/temp_places.csv", headers: true)
    taxonomy_hash = {}

    CSV.open("#{@output_path}/relationships.csv", 'w') do |csv_out|
      csv_out << @relationship_headers

      places.each do |pl|
        if pl['tags']
          tags = pl['tags'].split(',')
          tags.each do |tag|
            clean_tag = tag.strip

            unless taxonomy_hash[clean_tag]
              taxonomy_hash[clean_tag] = SecureRandom.uuid
            end

            csv_out << [
              @env['PROJECT_MODEL_RELATIONSHIP_ID_PLACES_TAXONOMIES'],
              pl['uuid'],
              'CoreDataConnector::Place',
              taxonomy_hash[clean_tag],
              'CoreDataConnector::Taxonomy'
            ]
          end
        end
      end
    end

    CSV.open("#{@output_path}/taxonomies.csv", 'w') do |csv_out|
      csv_out << ['project_model_id', 'uuid', 'name']

      taxonomy_hash.keys.each do |ti|
        csv_out << [
          @env['PROJECT_MODEL_ID_TAXONOMIES'],
          taxonomy_hash[ti],
          ti
        ]
      end
    end
  end
end

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
  'Dublin Core:Description',
  'Dublin Core:Type',
  'Dublin Core:Identifier',
  'Item Type Metadata:TopoName',
  'Item Type Metadata:URL'
]

fields = {
  places: {
    'name': 'Dublin Core:Title',
    'latitude': 'Item Type Metadata:geolocation:latitude',
    'longitude': 'Item Type Metadata:geolocation:longitude',
    'tags': 'tags'
  }
}

# Fill the fields.places object with more
column_names.each do |col|
  fields[:places]["udf_#{env[get_udf_key(col)]}"] = col
end

# Run the importer
transform = CsvTransform.new(
  input: options[:input],
  output: options[:output],
  env: env,
  fields: fields,
  model_files: model_files,
  id_column: 'Item Id'
)

transform.parse_models
transform.populate_taxonomies
transform.cleanup(['places'], ['original_id', 'tags'])

files = [
  "#{options[:output]}/places.csv",
  "#{options[:output]}/taxonomies.csv",
  "#{options[:output]}/relationships.csv"
]

archive = Archive.new
archive.create_archive(files, options[:output])
