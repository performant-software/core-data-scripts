#!/usr/bin/env ruby
# setup_project.rb — Create the RelNet FairData project via API
#
# Usage:
#   ruby scripts/relnet/setup_project.rb <email> <password> [environment] [project_id]
#
# Authenticates via email/password to the FairData API, creates the RelNet
# project with its models (+ UDFs) and relationships, then writes real IDs to
# .env.<environment>. Idempotent: matches existing project/models/relationships
# by name before creating.
#
# Adapted from scripts/marronage/setup_project.rb. See that file's comments for
# the API quirks (Clerk bypass header, column_name vs label, table_name
# requirement, order-based UDF UUID matching).
#
# RelNet model design — see relnet/fairdata-model.md. Categorical fields are
# modeled as Taxonomy + relationship (recommended in the pstudio-data-model
# skill: relationship names are auto-faceted in Typesense) rather than Select
# UDFs. This is the recommended design pending final confirmation with the
# client; flip individual categories to Select UDFs here if that changes.

require_relative '../../core/fairdata_api'

# Staging/prod run Clerk; FairDataApi::Client sends the `User-Agent: node`
# bypass on every request so auth routes to JWT/password.
BASE_URL = 'https://staging.coredata.cloud'

PROJECT_NAME = 'RelNet'
PROJECT_DESCRIPTION = 'Religious networks and sacred itineraries in late-1st-millennium-BCE Babylonia (University of Barcelona, PI Rocío Da Riva). Migrated from nodegoat.'

# --- HTTP + UDF helpers (delegate to the shared core/fairdata_api.rb) ---

CLIENT = FairDataApi::Client.new(BASE_URL)

def login(email, password)
  CLIENT.login(email, password)
end

# `_token` is ignored — CLIENT holds the JWT after login. Call sites are
# unchanged: api(:get, path, token) / api(:post, path, token, body) / etc.
def api(method, path, _token = nil, body = nil)
  body ? CLIENT.public_send(method, path, body) : CLIENT.public_send(method, path)
end

def map_udfs_by_order(udf_defs, api_udfs)
  FairDataApi.map_udfs_by_order(udf_defs, api_udfs)
end

# --- Model definitions ---
# UDF data_type ∈ String | Text | RichText | Number | Boolean | Select | FuzzyDate

MODELS = [
  {
    name: 'Places', name_singular: 'Place',
    model_class: 'CoreDataConnector::Place',
    udfs: [
      { label: 'Alternative Names', data_type: 'Text',     env: 'UDF_PLACES_ALT_NAMES' },
      { label: 'Description',       data_type: 'RichText', env: 'UDF_PLACES_DESCRIPTION' },
      { label: 'Pleiades ID',       data_type: 'String',   env: 'UDF_PLACES_PLEIADES_ID' },
      { label: 'Additional URLs',   data_type: 'String',   env: 'UDF_PLACES_ADDITIONAL_URLS' },
      { label: 'Nodegoat ID',       data_type: 'String',   env: 'UDF_PLACES_NODEGOAT_ID' },
    ]
  },
  {
    name: 'Tablets', name_singular: 'Tablet',
    model_class: 'CoreDataConnector::Item',
    udfs: [
      { label: 'RelNet ID',            data_type: 'String',   env: 'UDF_TABLETS_RELNET_ID' },
      { label: 'Text No.',             data_type: 'String',   env: 'UDF_TABLETS_TEXT_NO' },
      { label: 'Publication No.',      data_type: 'String',   env: 'UDF_TABLETS_PUBLICATION_NO' },
      { label: 'Joined Tablet',        data_type: 'Text',     env: 'UDF_TABLETS_JOINED_TABLET' },
      { label: 'Museum Collection',    data_type: 'String',   env: 'UDF_TABLETS_MUSEUM_COLLECTION' },
      { label: 'Colophons',            data_type: 'Text',     env: 'UDF_TABLETS_COLOPHONS' },
      { label: 'Rubrics',              data_type: 'Text',     env: 'UDF_TABLETS_RUBRICS' },
      { label: 'Measurements',         data_type: 'String',   env: 'UDF_TABLETS_MEASUREMENTS' },
      { label: 'Summary',              data_type: 'RichText', env: 'UDF_TABLETS_SUMMARY' },
      { label: 'Notes',                data_type: 'Text',     env: 'UDF_TABLETS_NOTES' },
      { label: 'Commentary',           data_type: 'Text',     env: 'UDF_TABLETS_COMMENTARY' },
      { label: 'Commentary to Tablet', data_type: 'Text',     env: 'UDF_TABLETS_COMMENTARY_TABLET' },
      { label: 'Date',                 data_type: 'String',   env: 'UDF_TABLETS_DATE' },        # original cuneiform-era string
      { label: 'Date (Gregorian)',     data_type: 'FuzzyDate', env: 'UDF_TABLETS_DATE_GREGORIAN' }, # drives timeline year_facet
      { label: 'Checked',              data_type: 'Boolean',  env: 'UDF_TABLETS_CHECKED' },
      { label: 'Nodegoat ID',          data_type: 'String',   env: 'UDF_TABLETS_NODEGOAT_ID' },
    ]
  },
  {
    name: 'Museums', name_singular: 'Museum',
    model_class: 'CoreDataConnector::Organization',
    udfs: [
      { label: 'URL',         data_type: 'String', env: 'UDF_MUSEUMS_URL' },
      { label: 'Nodegoat ID', data_type: 'String', env: 'UDF_MUSEUMS_NODEGOAT_ID' },
    ]
  },
  {
    name: 'Cultic Actors', name_singular: 'Cultic Actor',
    model_class: 'CoreDataConnector::Person',
    udfs: [
      { label: 'Alternative Names', data_type: 'Text',   env: 'UDF_CULTIC_ALT_NAMES' },
      { label: 'Nodegoat ID',       data_type: 'String', env: 'UDF_CULTIC_NODEGOAT_ID' },
    ]
  },
  {
    name: 'Divine Characters', name_singular: 'Divine Character',
    model_class: 'CoreDataConnector::Person',
    udfs: [
      { label: 'Alternative Names', data_type: 'Text',    env: 'UDF_DIVINE_ALT_NAMES' },
      { label: 'Function',          data_type: 'Text',    env: 'UDF_DIVINE_FUNCTION' },
      { label: 'Display',           data_type: 'Boolean', env: 'UDF_DIVINE_DISPLAY' }, # curation flag for selective public display
      { label: 'Nodegoat ID',       data_type: 'String',  env: 'UDF_DIVINE_NODEGOAT_ID' },
    ]
  },
  # --- Taxonomies (controlled vocabularies, faceted via relationships) ---
  { name: 'Place Types',     name_singular: 'Place Type',     model_class: 'CoreDataConnector::Taxonomy', udfs: [] },
  { name: 'Writing Classifications', name_singular: 'Writing Classification', model_class: 'CoreDataConnector::Taxonomy', udfs: [] },
  { name: 'Genders',         name_singular: 'Gender',         model_class: 'CoreDataConnector::Taxonomy', udfs: [] },
  { name: 'Divine Capacities', name_singular: 'Divine Capacity', model_class: 'CoreDataConnector::Taxonomy', udfs: [] },
]

# Relationships reference models by name; resolved to IDs after model creation.
# Phase A+ (data in hand now) and the Phase B "mentioned" cross-refs (defined so
# the structure is ready; populated once Rocío delivers the tablet multi-value
# exports).
RELATIONSHIPS = [
  # Phase A+ — importable now
  { primary: 'Tablets', related: 'Museums', name: 'Held at',  multiple: false, env: 'REL_TABLET_MUSEUM' },
  { primary: 'Tablets', related: 'Places',  name: 'Findspot', multiple: false, env: 'REL_TABLET_FINDSPOT' },
  { primary: 'Places',  related: 'Places',  name: 'Part of',  multiple: false, env: 'REL_PLACE_PARTOF' },
  { primary: 'Places',  related: 'Place Types', name: 'Kind of Place', multiple: false, env: 'REL_PLACE_KIND' },
  { primary: 'Tablets', related: 'Writing Classifications', name: 'Writing', multiple: false, env: 'REL_TABLET_WRITING' },
  { primary: 'Cultic Actors',    related: 'Genders', name: 'Gender', multiple: false, env: 'REL_CULTIC_GENDER' },
  { primary: 'Divine Characters', related: 'Genders', name: 'Gender', multiple: false, env: 'REL_DIVINE_GENDER' },
  { primary: 'Divine Characters', related: 'Divine Capacities', name: 'Capacity', multiple: false, env: 'REL_DIVINE_CAPACITY' },
  { primary: 'Divine Characters', related: 'Divine Characters', name: 'Alternative name for', multiple: false, env: 'REL_DIVINE_ALT_NAME_FOR' },
  # Phase B — defined now, populated when tablet cross-ref exports arrive
  { primary: 'Tablets', related: 'Places',            name: 'Places mentioned',            multiple: true, env: 'REL_TABLET_PLACES_MENTIONED' },
  { primary: 'Tablets', related: 'Divine Characters', name: 'Divine Characters mentioned', multiple: true, env: 'REL_TABLET_DIVINE_MENTIONED' },
  { primary: 'Tablets', related: 'Cultic Actors',     name: 'Cultic Actors mentioned',     multiple: true, env: 'REL_TABLET_CULTIC_MENTIONED' },
]

# --- Main ---

email = ARGV[0] or raise "Usage: ruby setup_project.rb <email> <password> [environment] [project_id]"
password = ARGV[1] or raise "Usage: ruby setup_project.rb <email> <password> [environment] [project_id]"
environment = ARGV[2] || 'staging'
forced_project_id = ARGV[3]&.to_i

puts "Logging in as #{email}..."
token = login(email, password)
puts "Authenticated."

# 1. Find or create the project
if forced_project_id
  puts "\nUsing project ID #{forced_project_id}..."
  project_id = forced_project_id
else
  puts "\nLooking for #{PROJECT_NAME} project..."
  projects = api(:get, '/core_data/projects', token)
  project_list = projects['projects'] || projects
  project = project_list.find { |p| p['name'] == PROJECT_NAME }

  if project
    puts "  Found existing project: ID #{project['id']}"
  else
    puts "  Creating project..."
    result = api(:post, '/core_data/projects', token, {
      project: { name: PROJECT_NAME, description: PROJECT_DESCRIPTION }
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

    full = api(:get, "/core_data/project_models/#{pm['id']}", token)
    pm = full['project_model']
    udf_map = map_udfs_by_order(model_def[:udfs], pm['user_defined_fields'] || [])
    model_ids[model_def[:name]] = { id: pm['id'], udfs: udf_map }
  end
end

# 3. Create relationships
puts "\nCreating relationships..."
rel_ids = {}

RELATIONSHIPS.each do |rel_def|
  primary = model_ids[rel_def[:primary]]
  related = model_ids[rel_def[:related]]

  raise "Model not found: #{rel_def[:primary]}" unless primary
  raise "Model not found: #{rel_def[:related]}" unless related

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
          { related_model_id: related[:id], name: rel_def[:name], multiple: rel_def[:multiple] }
        ]
      }
    }
    api(:put, "/core_data/project_models/#{primary[:id]}", token, body)

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

lines = []
lines << "# RelNet — FairData #{environment.capitalize} Environment"
lines << "# Auto-generated by setup_project.rb on #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}"
lines << "# Project ID: #{project_id}"
lines << ""
lines << "# Model IDs (PlainCsvIngester looks up PROJECT_MODEL_ID_{FILENAME_UPCASE})"
{
  'PLACES' => 'Places', 'TABLETS' => 'Tablets', 'MUSEUMS' => 'Museums',
  'CULTIC_ACTORS' => 'Cultic Actors', 'DIVINE_CHARACTERS' => 'Divine Characters',
  'PLACE_TYPES' => 'Place Types', 'WRITING_CLASSIFICATIONS' => 'Writing Classifications',
  'GENDERS' => 'Genders', 'DIVINE_CAPACITIES' => 'Divine Capacities'
}.each { |key, name| lines << "PROJECT_MODEL_ID_#{key}=#{model_ids[name][:id]}" }

lines << ""
lines << "# Relationship IDs"
RELATIONSHIPS.each { |r| lines << "#{r[:env]}=#{rel_ids[r[:env]]}" }

lines << ""
lines << "# UDF UUIDs"
MODELS.each do |model_def|
  next if model_def[:udfs].empty?
  lines << "# #{model_def[:name]}"
  model_def[:udfs].each { |udf| lines << "#{udf[:env]}=#{model_ids[model_def[:name]][:udfs][udf[:env]]}" }
end

File.write(env_file, lines.join("\n") + "\n")
puts "Done! .env.#{environment} written with real IDs."
puts "\nNext steps:"
puts "  1. ruby scripts/relnet/csv_import.rb #{environment}"
puts "  2. Verify UDF UUIDs map to the right column_name via GET /core_data/project_models/{id}"
puts "  3. Import output CSVs via FairData admin UI (staging first)"
