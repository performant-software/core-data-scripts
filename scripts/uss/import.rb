require 'aws-sdk-s3'
require 'csv'
require 'optparse'
require 'securerandom'

require_relative '../../core/archive'
require_relative '../../core/env'

DIRECTORY_NAME = 'uss'

class Import
  attr_reader :client, :output_path, :env

  def initialize(endpoint, access_key, secret_key, output_path, env)
    @output_path = output_path
    @env = env

    initialize_client endpoint, access_key, secret_key
  end

  def run
    bucket = env['BUCKET_NAME']
    prefix = env['OBJECT_PREFIX']

    items = [%w[project_model_id uuid name]]
    media_contents = [%w[project_model_id uuid name url content_warning]]
    relationships = [%w[project_model_relationship_id uuid primary_record_uuid primary_record_type related_record_uuid related_record_type]]

    results = client.list_objects(bucket:, prefix:)

    results.contents.each do |o|
      object = Aws::S3::Object.new(bucket, o.key)

      file_name = object.key.gsub("#{prefix}/", '')
      document_name = file_name.gsub('.pdf', '')
      url = object_url(bucket, o.key)

      item_uuid = SecureRandom.uuid
      media_content_uuid = SecureRandom.uuid
      relationship_uuid = SecureRandom.uuid

      items << [env['DOCUMENTS_MODEL'].to_i, item_uuid, document_name]
      media_contents << [env['FILES_MODEL'].to_i, media_content_uuid, file_name, url, false]
      relationships << [env['DOCUMENTS_FILE_RELATIONSHIP'].to_i, relationship_uuid, item_uuid, 'CoreDataConnector::Item', media_content_uuid, 'CoreDataConnector::MediaContent']
    end

    write_file "#{output_path}/items.csv", items
    write_file "#{output_path}/media_contents.csv", media_contents
    write_file "#{output_path}/relationships.csv", relationships

    %W[#{output_path}/items.csv #{output_path}/media_contents.csv #{output_path}/relationships.csv]
  end

  private

  def initialize_client(endpoint, access_key_id, secret_access_key)
    @client = Aws::S3::Client.new(
      access_key_id:,
      secret_access_key:,
      endpoint:,
      force_path_style: false,
      region: 'us-east-1'
    )
  end

  def object_url(bucket, key)
    "https://#{bucket}.nyc3.digitaloceanspaces.com/#{key.gsub(' ', '%20')}"
  end

  def write_file(filename, rows)
    CSV.open(filename, 'w') do |csv|
      rows.each { |r| csv << r }
    end
  end
end


# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on('-p', '--endpoint ARG', String) { |endpoint| options[:endpoint] = endpoint }
  opts.on('-a', '--access-key ARG', String) { |access_key| options[:access_key] = access_key }
  opts.on('-s', '--secret-key ARG', String) { |secret_key| options[:secret_key] = secret_key }
  opts.on('-o', '--output ARG', String) { |output| options[:output] = output }
  opts.on('-e', '--environment ARG', String) { |environment| options[:environment] = environment }
end.parse!(ARGV)

# Parse environment variables
env_manager = Env.new
env = env_manager.initialize_env('./scripts/uss', options[:environment])

# Run the import generation script
import = Import.new(
  options[:endpoint],
  options[:access_key],
  options[:secret_key],
  options[:output],
  env
)

filepaths = import.run

archive = Archive.new
archive.create_archive(filepaths, options[:output])