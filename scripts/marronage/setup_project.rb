#!/usr/bin/env ruby
# setup_project.rb — Create the Marronage FairData project via API
#
# Usage:
#   ruby scripts/marronage/setup_project.rb <email> <password> [environment]
#
# Authenticates via email/password to the FairData API, creates a project
# with 10 models (with UDFs) and 11 relationships, then writes real IDs
# to .env.<environment>.
#
# Idempotent: checks for existing project/models by name before creating.

require 'net/http'
require 'json'
require 'uri'

BASE_URL = 'https://staging.coredata.cloud'
BYPASS_HEADER = { 'access-control-expose-headers' => 'x-trigger-jwt' }

# --- HTTP helpers ---

def api(method, path, token, body = nil)
  uri = URI("#{BASE_URL}#{path}")
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true

  req = case method
        when :get    then Net::HTTP::Get.new(uri)
        when :post   then Net::HTTP::Post.new(uri)
        when :put    then Net::HTTP::Put.new(uri)
        when :patch  then Net::HTTP::Patch.new(uri)
        when :delete then Net::HTTP::Delete.new(uri)
        end

  req['Authorization'] = "Bearer #{token}"
  req['Content-Type'] = 'application/json'
  req['Accept'] = 'application/json'
  BYPASS_HEADER.each { |k, v| req[k] = v }

  req.body = body.to_json if body

  res = http.request(req)
  unless res.code.to_i.between?(200, 299)
    raise "API #{method.upcase} #{path} returned #{res.code}: #{res.body&.slice(0, 500)}"
  end

  JSON.parse(res.body) rescue {}
end

def login(email, password)
  uri = URI("#{BASE_URL}/auth/login")
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true

  req = Net::HTTP::Post.new(uri)
  req['Content-Type'] = 'application/json'
  BYPASS_HEADER.each { |k, v| req[k] = v }
  req.body = { email: email, password: password }.to_json

  res = http.request(req)
  raise "Login failed (#{res.code}): #{res.body}" unless res.code == '200'

  JSON.parse(res.body)['token']
end

# --- UDF matching helper ---
# The API doesn't return UDF labels, so match by order (which we control at creation time)
def map_udfs_by_order(udf_defs, api_udfs)
  sorted = api_udfs.sort_by { |u| u['order'].to_i }
  udf_map = {}
  udf_defs.each_with_index do |udf_def, i|
    if sorted[i]
      udf_map[udf_def[:env]] = sorted[i]['uuid']
    end
  end
  udf_map
end

# --- Model definitions ---

MODELS = [
  {
    name: 'Settlements', name_singular: 'Settlement',
    model_class: 'CoreDataConnector::Place',
    udfs: [
      { label: 'PID',                    data_type: 'String',   env: 'UDF_SETTLEMENTS_PID' },
      { label: 'Location Landmark',      data_type: 'String',   env: 'UDF_SETTLEMENTS_LANDMARK' },
      { label: 'Historical Description', data_type: 'RichText', env: 'UDF_SETTLEMENTS_HISTORICAL_DESC' },
      { label: 'Known Population',       data_type: 'Text',     env: 'UDF_SETTLEMENTS_POPULATION' },
      { label: 'Dilemmas and Issues',    data_type: 'RichText', env: 'UDF_SETTLEMENTS_DILEMMAS' },
      { label: 'Geography Description',  data_type: 'RichText', env: 'UDF_SETTLEMENTS_GEOGRAPHY_DESC' },
    ]
  },
  {
    name: 'Events', name_singular: 'Event',
    model_class: 'CoreDataConnector::Event',
    udfs: [
      { label: 'PID Event',          data_type: 'String', env: 'UDF_EVENTS_PID' },
      { label: 'Origin Report Date', data_type: 'String', env: 'UDF_EVENTS_ORIGIN_REPORT_DATE' },
      { label: 'Change Report Date', data_type: 'String', env: 'UDF_EVENTS_CHANGE_REPORT_DATE' },
    ]
  },
  {
    name: 'Sources', name_singular: 'Source',
    model_class: 'CoreDataConnector::Item',
    udfs: [
      { label: 'PID Source',      data_type: 'String',   env: 'UDF_SOURCES_PID' },
      { label: 'Full Reference',  data_type: 'RichText', env: 'UDF_SOURCES_FULL_REFERENCE' },
    ]
  },
  {
    name: 'Colonial Landscape', name_singular: 'Colonial Landscape Feature',
    model_class: 'CoreDataConnector::Place',
    udfs: [
      { label: 'PID',            data_type: 'String', env: 'UDF_CL_PID' },
      { label: 'State/Province', data_type: 'String', env: 'UDF_CL_STATE' },
      { label: 'Start Date',    data_type: 'String', env: 'UDF_CL_START_DATE' },
      { label: 'End Date',      data_type: 'String', env: 'UDF_CL_END_DATE' },
    ]
  },
  {
    name: 'Group Types', name_singular: 'Group Type',
    model_class: 'CoreDataConnector::Taxonomy',
    udfs: []
  },
  {
    name: 'Event Types', name_singular: 'Event Type',
    model_class: 'CoreDataConnector::Taxonomy',
    udfs: [
      { label: 'Description', data_type: 'Text', env: 'UDF_EVENT_TYPES_DESCRIPTION' },
    ]
  },
  {
    name: 'Location Accuracy', name_singular: 'Location Accuracy Level',
    model_class: 'CoreDataConnector::Taxonomy',
    udfs: []
  },
  {
    name: 'Landscape Types', name_singular: 'Landscape Type',
    model_class: 'CoreDataConnector::Taxonomy',
    udfs: []
  },
  {
    name: 'Countries', name_singular: 'Country',
    model_class: 'CoreDataConnector::Place',
    udfs: []
  },
  {
    name: 'States/Provinces', name_singular: 'State/Province',
    model_class: 'CoreDataConnector::Place',
    udfs: []
  },
]

# Relationships reference models by name; resolved to IDs after model creation
RELATIONSHIPS = [
  { primary: 'Settlements', related: 'Group Types',       name: 'Group Type',        multiple: false, env: 'REL_SETTLEMENTS_GROUP_TYPES' },
  { primary: 'Settlements', related: 'Location Accuracy', name: 'Location Accuracy',  multiple: false, env: 'REL_SETTLEMENTS_LOCATION_ACCURACY' },
  { primary: 'Settlements', related: 'Events',            name: 'Events',             multiple: true,  env: 'REL_SETTLEMENTS_EVENTS' },
  { primary: 'Settlements', related: 'Sources',           name: 'Sources',            multiple: true,  env: 'REL_SETTLEMENTS_SOURCES' },
  { primary: 'Events',      related: 'Event Types',       name: 'Start Event Type',   multiple: false, env: 'REL_EVENTS_START_EVENT_TYPE' },
  { primary: 'Events',      related: 'Event Types',       name: 'Change Event Type',  multiple: false, env: 'REL_EVENTS_CHANGE_EVENT_TYPE' },
  { primary: 'Events',      related: 'Sources',           name: 'Sources',            multiple: true,  env: 'REL_EVENTS_SOURCES' },
  { primary: 'Settlements', related: 'Countries',          name: 'Country',        multiple: false, env: 'REL_SETTLEMENTS_COUNTRIES' },
  { primary: 'Settlements', related: 'States/Provinces',   name: 'State/Province', multiple: true,  env: 'REL_SETTLEMENTS_STATES' },
  { primary: 'Colonial Landscape', related: 'Landscape Types', name: 'Landscape Type', multiple: false, env: 'REL_COLONIAL_LANDSCAPE_TYPES' },
  { primary: 'Colonial Landscape', related: 'Countries',   name: 'Country',        multiple: false, env: 'REL_CL_COUNTRIES' },
]

# --- Main ---

email = ARGV[0] or raise "Usage: ruby setup_project.rb <email> <password> [environment] [project_id]"
password = ARGV[1] or raise "Usage: ruby setup_project.rb <email> <password> [environment] [project_id]"
environment = ARGV[2] || 'development'
forced_project_id = ARGV[3]&.to_i

puts "Logging in as #{email}..."
token = login(email, password)
puts "Authenticated."

# 1. Find or create the project
if forced_project_id
  puts "\nUsing project ID #{forced_project_id}..."
  project_id = forced_project_id
else
  puts "\nLooking for Marronage project..."
  projects = api(:get, '/core_data/projects', token)
  project_list = projects['projects'] || projects
  project = project_list.find { |p| p['name'] == 'Marronage Mapping Project' }

  if project
    puts "  Found existing project: ID #{project['id']}"
  else
    puts "  Creating project..."
    result = api(:post, '/core_data/projects', token, {
      project: { name: 'Marronage Mapping Project', description: 'NocoDB/ArcGIS migration — Caribbean maroon settlements' }
    })
    project = result['project']
    puts "  Created project: ID #{project['id']}"
  end

  project_id = project['id']
end

# 2. Create models with UDFs
puts "\nCreating models..."
model_ids = {}  # name -> { id:, udfs: { env_key: uuid } }

existing_models = api(:get, "/core_data/project_models?project_id=#{project_id}", token)
existing_list = existing_models['project_models'] || []

MODELS.each do |model_def|
  existing = existing_list.find { |m| m['name'] == model_def[:name] }

  if existing
    puts "  #{model_def[:name]}: already exists (ID #{existing['id']})"
    # Fetch full model to get UDF UUIDs
    full = api(:get, "/core_data/project_models/#{existing['id']}", token)
    pm = full['project_model']
    udf_map = map_udfs_by_order(model_def[:udfs], pm['user_defined_fields'] || [])
    model_ids[model_def[:name]] = { id: pm['id'], udfs: udf_map }
  else
    udf_attrs = model_def[:udfs].each_with_index.map do |udf, i|
      { column_name: udf[:label], data_type: udf[:data_type], table_name: model_def[:model_class], required: false, order: i }
    end

    body = {
      project_model: {
        project_id: project_id,
        name: model_def[:name],
        name_singular: model_def[:name_singular],
        model_class: model_def[:model_class],
        user_defined_fields_attributes: udf_attrs
      }
    }

    result = api(:post, '/core_data/project_models', token, body)
    pm = result['project_model']
    puts "  #{model_def[:name]}: created (ID #{pm['id']})"

    # Read back to get UDF UUIDs
    full = api(:get, "/core_data/project_models/#{pm['id']}", token)
    pm = full['project_model']
    udf_map = map_udfs_by_order(model_def[:udfs], pm['user_defined_fields'] || [])
    model_ids[model_def[:name]] = { id: pm['id'], udfs: udf_map }
  end
end

# 3. Create relationships
puts "\nCreating relationships..."
rel_ids = {}  # env_key -> id

RELATIONSHIPS.each do |rel_def|
  primary = model_ids[rel_def[:primary]]
  related = model_ids[rel_def[:related]]

  raise "Model not found: #{rel_def[:primary]}" unless primary
  raise "Model not found: #{rel_def[:related]}" unless related

  # Check if relationship already exists on the primary model
  full = api(:get, "/core_data/project_models/#{primary[:id]}", token)
  pm = full['project_model']
  existing_rels = pm['project_model_relationships'] || []
  existing_rel = existing_rels.find { |r| r['name'] == rel_def[:name] && r['related_model_id'] == related[:id] }

  if existing_rel
    puts "  #{rel_def[:name]} (#{rel_def[:primary]} -> #{rel_def[:related]}): already exists (ID #{existing_rel['id']})"
    rel_ids[rel_def[:env]] = existing_rel['id']
  else
    body = {
      project_model: {
        project_model_relationships_attributes: [
          {
            related_model_id: related[:id],
            name: rel_def[:name],
            multiple: rel_def[:multiple]
          }
        ]
      }
    }

    api(:put, "/core_data/project_models/#{primary[:id]}", token, body)

    # Read back to get the relationship ID
    full = api(:get, "/core_data/project_models/#{primary[:id]}", token)
    pm = full['project_model']
    new_rel = (pm['project_model_relationships'] || []).find { |r| r['name'] == rel_def[:name] && r['related_model_id'] == related[:id] }
    raise "Failed to create relationship: #{rel_def[:name]}" unless new_rel

    puts "  #{rel_def[:name]} (#{rel_def[:primary]} -> #{rel_def[:related]}): created (ID #{new_rel['id']})"
    rel_ids[rel_def[:env]] = new_rel['id']
  end
end

# 4. Write .env file
env_file = File.expand_path("./.env.#{environment}", __dir__)
puts "\nWriting #{env_file}..."

env_content = <<~ENV
  # Marronage Mapping Project — FairData #{environment.capitalize} Environment
  # Auto-generated by setup_project.rb on #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}
  # Project ID: #{project_id}

  # Model IDs
  PROJECT_MODEL_ID_SETTLEMENTS=#{model_ids['Settlements'][:id]}
  PROJECT_MODEL_ID_EVENTS=#{model_ids['Events'][:id]}
  PROJECT_MODEL_ID_SOURCES=#{model_ids['Sources'][:id]}
  PROJECT_MODEL_ID_COLONIAL_LANDSCAPE=#{model_ids['Colonial Landscape'][:id]}
  PROJECT_MODEL_ID_GROUP_TYPES=#{model_ids['Group Types'][:id]}
  PROJECT_MODEL_ID_EVENT_TYPES=#{model_ids['Event Types'][:id]}
  PROJECT_MODEL_ID_LOCATION_ACCURACY=#{model_ids['Location Accuracy'][:id]}
  PROJECT_MODEL_ID_LANDSCAPE_TYPES=#{model_ids['Landscape Types'][:id]}
  PROJECT_MODEL_ID_COUNTRIES=#{model_ids['Countries'][:id]}
  PROJECT_MODEL_ID_STATES_PROVINCES=#{model_ids['States/Provinces'][:id]}

  # Relationship IDs
  REL_SETTLEMENTS_GROUP_TYPES=#{rel_ids['REL_SETTLEMENTS_GROUP_TYPES']}
  REL_SETTLEMENTS_LOCATION_ACCURACY=#{rel_ids['REL_SETTLEMENTS_LOCATION_ACCURACY']}
  REL_SETTLEMENTS_EVENTS=#{rel_ids['REL_SETTLEMENTS_EVENTS']}
  REL_SETTLEMENTS_SOURCES=#{rel_ids['REL_SETTLEMENTS_SOURCES']}
  REL_EVENTS_START_EVENT_TYPE=#{rel_ids['REL_EVENTS_START_EVENT_TYPE']}
  REL_EVENTS_CHANGE_EVENT_TYPE=#{rel_ids['REL_EVENTS_CHANGE_EVENT_TYPE']}
  REL_EVENTS_SOURCES=#{rel_ids['REL_EVENTS_SOURCES']}
  REL_SETTLEMENTS_COUNTRIES=#{rel_ids['REL_SETTLEMENTS_COUNTRIES']}
  REL_SETTLEMENTS_STATES=#{rel_ids['REL_SETTLEMENTS_STATES']}
  REL_COLONIAL_LANDSCAPE_TYPES=#{rel_ids['REL_COLONIAL_LANDSCAPE_TYPES']}
  REL_CL_COUNTRIES=#{rel_ids['REL_CL_COUNTRIES']}

  # UDF UUIDs — Settlements
  UDF_SETTLEMENTS_PID=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_PID']}
  UDF_SETTLEMENTS_LANDMARK=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_LANDMARK']}
  UDF_SETTLEMENTS_HISTORICAL_DESC=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_HISTORICAL_DESC']}
  UDF_SETTLEMENTS_POPULATION=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_POPULATION']}
  UDF_SETTLEMENTS_DILEMMAS=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_DILEMMAS']}
  UDF_SETTLEMENTS_GEOGRAPHY_DESC=#{model_ids['Settlements'][:udfs]['UDF_SETTLEMENTS_GEOGRAPHY_DESC']}

  # UDF UUIDs — Events
  UDF_EVENTS_PID=#{model_ids['Events'][:udfs]['UDF_EVENTS_PID']}
  UDF_EVENTS_ORIGIN_REPORT_DATE=#{model_ids['Events'][:udfs]['UDF_EVENTS_ORIGIN_REPORT_DATE']}
  UDF_EVENTS_CHANGE_REPORT_DATE=#{model_ids['Events'][:udfs]['UDF_EVENTS_CHANGE_REPORT_DATE']}

  # UDF UUIDs — Sources
  UDF_SOURCES_PID=#{model_ids['Sources'][:udfs]['UDF_SOURCES_PID']}
  UDF_SOURCES_FULL_REFERENCE=#{model_ids['Sources'][:udfs]['UDF_SOURCES_FULL_REFERENCE']}

  # UDF UUIDs — Colonial Landscape
  UDF_CL_PID=#{model_ids['Colonial Landscape'][:udfs]['UDF_CL_PID']}
  UDF_CL_STATE=#{model_ids['Colonial Landscape'][:udfs]['UDF_CL_STATE']}
  UDF_CL_START_DATE=#{model_ids['Colonial Landscape'][:udfs]['UDF_CL_START_DATE']}
  UDF_CL_END_DATE=#{model_ids['Colonial Landscape'][:udfs]['UDF_CL_END_DATE']}

  # UDF UUIDs — Event Types
  UDF_EVENT_TYPES_DESCRIPTION=#{model_ids['Event Types'][:udfs]['UDF_EVENT_TYPES_DESCRIPTION']}
ENV

File.write(env_file, env_content)
puts "Done! .env.#{environment} written with real IDs."
puts "\nNext steps:"
puts "  1. ruby scripts/marronage/csv_import.rb #{environment}"
puts "  2. ruby scripts/marronage/validate.rb"
puts "  3. Import output CSVs via FairData admin UI"
