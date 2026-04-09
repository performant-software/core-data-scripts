require 'csv'
require 'json'
require 'securerandom'
require 'fileutils'
require 'dotenv'
require_relative '../../core/csv/plain_csv_ingester'

env = Dotenv.parse(File.expand_path("./.env.#{ARGV[0] || 'development'}", __dir__))

# Normalize state/province values from NocoDB MultiSelect field
def normalize_state(val)
  corrections = {
    'Falcón sate' => 'Falcón state',
    'Lara State' => 'Lara state',
    'Vargas State' => 'Vargas state',
  }
  corrections[val] || val
end

input = File.expand_path('data/marronage/input')
output = File.expand_path('data/marronage/output')
intermediate = File.expand_path('data/marronage/intermediate')

FileUtils.mkdir_p(intermediate)
FileUtils.mkdir_p(output)

# ---------------------------------------------------------------------------
# 1. merge_settlements_with_descriptions
#    Reads geocimarronaje-main and descriptions CSVs, merges on pid,
#    writes intermediate/settlements.csv
# ---------------------------------------------------------------------------
def merge_settlements_with_descriptions(input, intermediate)
  main_file = Dir.glob("#{input}/geocimarronaje-main*.csv").first
  desc_file  = Dir.glob("#{input}/descriptions*.csv").first

  raise "geocimarronaje-main CSV not found in #{input}" unless main_file
  raise "descriptions CSV not found in #{input}" unless desc_file

  # Index descriptions by pid
  descriptions = {}
  CSV.foreach(desc_file, headers: true, encoding: 'bom|utf-8') do |row|
    pid = row['pid']
    next if pid.nil? || pid.strip.empty?
    descriptions[pid.strip] = row
  end

  output_path = File.join(intermediate, 'settlements.csv')
  columns = %w[
    id name latitude longitude location_landmark
    present_location_state_or_province present_location_country
    location_accuracy group_type
    historical_description known_population dilemmas_issues geography_description
  ]

  CSV.open(output_path, 'w', headers: columns, write_headers: true) do |csv|
    CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
      pid = row['pid']
      next if pid.nil? || pid.strip.empty?
      pid = pid.strip

      desc = descriptions[pid]

      lat = row['latitude']
      lon = row['longitude']
      lat = nil if lat.nil? || lat.strip.downcase == 'n/a'
      lon = nil if lon.nil? || lon.strip.downcase == 'n/a'

      # Strip surrounding quotes from names (inconsistent in source data)
      name = row['name']&.strip
      name = name[1..-2] if name&.start_with?('"') && name&.end_with?('"')

      csv << [
        pid,
        name,
        lat,
        lon,
        row['location_landmark'],
        row['present_location_state or province'],
        row['present _location_country'],
        row['location_accuracy'],
        row['group_type'],
        desc ? desc['historical_description'] : nil,
        desc ? desc['known_population']       : nil,
        desc ? desc['dilemmas_issues']        : nil,
        desc ? desc['geography_description']  : nil
      ]
    end
  end

  puts "Wrote #{output_path}"
end

# ---------------------------------------------------------------------------
# 2. prepare_events
#    Reads events CSV, indexes settlement names from geocimarronaje-main,
#    constructs event names, handles duplicate pid_event e043,
#    writes intermediate/events.csv
# ---------------------------------------------------------------------------
def prepare_events(input, intermediate)
  events_file = Dir.glob("#{input}/events*.csv").first
  main_file   = Dir.glob("#{input}/geocimarronaje-main*.csv").first

  raise "events CSV not found in #{input}" unless events_file
  raise "geocimarronaje-main CSV not found in #{input}" unless main_file

  # Index settlement pids by name
  settlement_pid_by_name = {}
  CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
    name = row['name']
    pid  = row['pid']
    next if name.nil? || pid.nil?
    settlement_pid_by_name[name.strip] = pid.strip
  end

  output_path = File.join(intermediate, 'events.csv')
  columns = %w[
    id name start_date change_date origin_report_date change_report_date
    start_event change_event settlement_pid location_accuracy group_type
  ]

  # Collect raw rows first so we can detect duplicates
  rows = []
  CSV.foreach(events_file, headers: true, encoding: 'bom|utf-8') do |row|
    pid_event = row['pid_event']
    next if pid_event.nil? || pid_event.strip.empty?
    rows << row
  end

  # Find duplicate pid_event values
  pid_counts = Hash.new(0)
  rows.each { |r| pid_counts[r['pid_event'].strip] += 1 }
  duplicates = pid_counts.select { |_, count| count > 1 }.keys

  duplicates.each do |dup_pid|
    warn "WARNING: duplicate pid_event '#{dup_pid}' — disambiguating with _{settlement_pid} suffix"
  end

  CSV.open(output_path, 'w', headers: columns, write_headers: true) do |csv|
    rows.each do |row|
      pid_event      = row['pid_event'].strip
      settlement_name = row['pid-town lookup']&.strip
      settlement_pid  = settlement_pid_by_name[settlement_name]

      if settlement_pid.nil? && !settlement_name.nil?
        warn "WARNING: no settlement pid found for name '#{settlement_name}' (event #{pid_event})"
      end

      event_name = "#{row['start_event']}: #{settlement_name}"

      # Disambiguate duplicate pid_event
      id = if duplicates.include?(pid_event) && settlement_pid
             "#{pid_event}_#{settlement_pid}"
           else
             pid_event
           end

      csv << [
        id,
        event_name,
        row['start_date'],
        row['change_date'],
        row['origin_report_date'],
        row['change_report_date'],
        row['start_event'],
        row['change_event'],
        settlement_pid,
        row['location_accuracy (from geocimarronaje-main)'],
        row['Group Type']
      ]
    end
  end

  puts "Wrote #{output_path}"
end

# ---------------------------------------------------------------------------
# 3. extract_taxonomies
#    Extracts distinct controlled-vocabulary values into 4 taxonomy CSVs
# ---------------------------------------------------------------------------
def extract_taxonomies(input, intermediate)
  main_file       = Dir.glob("#{input}/geocimarronaje-main*.csv").first
  event_types_file = Dir.glob("#{input}/event_types_list*.csv").first
  landscape_file  = Dir.glob("#{input}/coloniallandscape*.csv").first

  raise "geocimarronaje-main CSV not found in #{input}" unless main_file
  raise "event_types_list CSV not found in #{input}" unless event_types_file
  raise "coloniallandscape CSV not found in #{input}" unless landscape_file

  # --- group_types ---
  group_types = []
  CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
    val = row['group_type']&.strip
    group_types << val if val && !val.empty? && !group_types.include?(val)
  end

  group_types_path = File.join(intermediate, 'group_types.csv')
  CSV.open(group_types_path, 'w', headers: %w[id group_type], write_headers: true) do |csv|
    group_types.each { |val| csv << [val, val] }
  end
  puts "Wrote #{group_types_path}"

  # --- location_accuracy ---
  accuracies = []
  CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
    val = row['location_accuracy']&.strip
    accuracies << val if val && !val.empty? && !accuracies.include?(val)
  end

  accuracy_path = File.join(intermediate, 'location_accuracy.csv')
  CSV.open(accuracy_path, 'w', headers: %w[id location_accuracy], write_headers: true) do |csv|
    accuracies.each { |val| csv << [val, val] }
  end
  puts "Wrote #{accuracy_path}"

  # --- event_types (from event_types_list CSV) ---
  event_types_path = File.join(intermediate, 'event_types.csv')
  CSV.open(event_types_path, 'w', headers: %w[id Event Description], write_headers: true) do |csv|
    CSV.foreach(event_types_file, headers: true, encoding: 'bom|utf-8') do |row|
      event_name = row['Event']&.strip
      next if event_name.nil? || event_name.empty?
      csv << [event_name, event_name, row['Description']]
    end
  end
  puts "Wrote #{event_types_path}"

  # --- landscape_types ---
  landscape_types = []
  CSV.foreach(landscape_file, headers: true, encoding: 'bom|utf-8') do |row|
    val = row['type']&.strip
    landscape_types << val if val && !val.empty? && !landscape_types.include?(val)
  end

  landscape_types_path = File.join(intermediate, 'landscape_types.csv')
  CSV.open(landscape_types_path, 'w', headers: %w[id type], write_headers: true) do |csv|
    landscape_types.each { |val| csv << [val, val] }
  end
  puts "Wrote #{landscape_types_path}"

  # --- countries (Place model) ---
  countries = []
  CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
    val = row['present _location_country']&.strip
    countries << val if val && !val.empty? && !countries.include?(val)
  end
  CSV.foreach(landscape_file, headers: true, encoding: 'bom|utf-8') do |row|
    val = row['present_location_country']&.strip
    countries << val if val && !val.empty? && !countries.include?(val)
  end

  countries_path = File.join(intermediate, 'countries.csv')
  CSV.open(countries_path, 'w', headers: %w[id name], write_headers: true) do |csv|
    countries.each { |val| csv << [val, val] }
  end
  puts "Wrote #{countries_path}"

  # --- states_provinces (Place model, split MultiSelect values) ---
  states = []
  CSV.foreach(main_file, headers: true, encoding: 'bom|utf-8') do |row|
    raw = row['present_location_state or province']&.strip
    next if raw.nil? || raw.empty?
    raw.split(',').map(&:strip).each do |val|
      val = normalize_state(val)
      states << val if !val.empty? && !states.include?(val)
    end
  end

  states_path = File.join(intermediate, 'states_provinces.csv')
  CSV.open(states_path, 'w', headers: %w[id name], write_headers: true) do |csv|
    states.each { |val| csv << [val, val] }
  end
  puts "Wrote #{states_path}"
end

# ---------------------------------------------------------------------------
# 4. prepare_sources
#    Reads sources CSV, writes intermediate/sources.csv
# ---------------------------------------------------------------------------
def prepare_sources(input, intermediate)
  sources_file = Dir.glob("#{input}/sources*.csv").first
  raise "sources CSV not found in #{input}" unless sources_file

  output_path = File.join(intermediate, 'sources.csv')
  columns = ['id', 'Full reference']

  CSV.open(output_path, 'w', headers: columns, write_headers: true) do |csv|
    CSV.foreach(sources_file, headers: true, encoding: 'bom|utf-8') do |row|
      pid = row['pid-source']&.strip
      next if pid.nil? || pid.empty?
      csv << [pid, row['Full reference']]
    end
  end

  puts "Wrote #{output_path}"
end

# ---------------------------------------------------------------------------
# 5. prepare_colonial_landscape
#    Reads coloniallandscape CSV, writes intermediate/colonial_landscape.csv
# ---------------------------------------------------------------------------
def prepare_colonial_landscape(input, intermediate)
  landscape_file = Dir.glob("#{input}/coloniallandscape*.csv").first
  raise "coloniallandscape CSV not found in #{input}" unless landscape_file

  output_path = File.join(intermediate, 'colonial_landscape.csv')
  columns = %w[
    id name type latitude longitude
    present_location_state_or_province present_location_country
    start_date end_date
  ]

  CSV.open(output_path, 'w', headers: columns, write_headers: true) do |csv|
    CSV.foreach(landscape_file, headers: true, encoding: 'bom|utf-8') do |row|
      pid = row['pid']&.strip
      next if pid.nil? || pid.empty?
      csv << [
        pid,
        row['name'],
        row['type'],
        row['latitude'],
        row['longitude'],
        row['present_location_state or province'],
        row['present_location_country'],
        row['start_date'],
        row['end_date']
      ]
    end
  end

  puts "Wrote #{output_path}"
end

# --- Run pre-processing ---
merge_settlements_with_descriptions(input, intermediate)
prepare_events(input, intermediate)
extract_taxonomies(input, intermediate)
prepare_sources(input, intermediate)
prepare_colonial_landscape(input, intermediate)

puts "\nPre-processing complete. Intermediate files in: #{intermediate}"

# --- FairData CSV Transform via PlainCsvIngester ---

fields = {
  settlements: {
    'name': 'name',
    'latitude': 'latitude',
    'longitude': 'longitude',
    "udf_#{env['UDF_SETTLEMENTS_PID'].tr('-','_')}": 'id',
    "udf_#{env['UDF_SETTLEMENTS_LANDMARK'].tr('-','_')}": 'location_landmark',
    "udf_#{env['UDF_SETTLEMENTS_HISTORICAL_DESC'].tr('-','_')}": 'historical_description',
    "udf_#{env['UDF_SETTLEMENTS_POPULATION'].tr('-','_')}": 'known_population',
    "udf_#{env['UDF_SETTLEMENTS_DILEMMAS'].tr('-','_')}": 'dilemmas_issues',
    "udf_#{env['UDF_SETTLEMENTS_GEOGRAPHY_DESC'].tr('-','_')}": 'geography_description'
  },
  events: {
    'name': 'name',
    'description': 'description',
    'start_date': 'start_date',
    'start_date_description': 'start_date_description',
    'end_date': 'change_date',
    'end_date_description': 'end_date_description',
    "udf_#{env['UDF_EVENTS_PID'].tr('-','_')}": 'id',
    "udf_#{env['UDF_EVENTS_ORIGIN_REPORT_DATE'].tr('-','_')}": 'origin_report_date',
    "udf_#{env['UDF_EVENTS_CHANGE_REPORT_DATE'].tr('-','_')}": 'change_report_date'
  },
  sources: {
    'name': 'id',
    "udf_#{env['UDF_SOURCES_PID'].tr('-','_')}": 'id',
    "udf_#{env['UDF_SOURCES_FULL_REFERENCE'].tr('-','_')}": 'Full reference'
  },
  colonial_landscape: {
    'name': 'name',
    'latitude': 'latitude',
    'longitude': 'longitude',
    "udf_#{env['UDF_CL_PID'].tr('-','_')}": 'id',
    "udf_#{env['UDF_CL_STATE'].tr('-','_')}": 'present_location_state_or_province',
    "udf_#{env['UDF_CL_START_DATE'].tr('-','_')}": 'start_date',
    "udf_#{env['UDF_CL_END_DATE'].tr('-','_')}": 'end_date'
  },
  group_types: {
    'name': 'group_type'
  },
  event_types: {
    'name': 'Event',
    "udf_#{env['UDF_EVENT_TYPES_DESCRIPTION'].tr('-','_')}": 'Description'
  },
  location_accuracy: {
    'name': 'location_accuracy'
  },
  landscape_types: {
    'name': 'type'
  },
  countries: {
    'name': 'name'
  },
  states_provinces: {
    'name': 'name'
  }
}

model_files = %w[
  settlements events sources colonial_landscape
  group_types event_types location_accuracy landscape_types
  countries states_provinces
]

transform = Csv::PlainCsvIngester.new(
  input: intermediate,
  output: output,
  id_map_path: File.expand_path('./id_maps/marronage', File.dirname(__FILE__)),
  env: env,
  fields: fields,
  model_files: model_files
)

transform.parse_models
transform.cleanup(model_files)

puts "FairData CSV transform complete. Output files in: #{output}"

# --- Post-processing: Combine output files for FairData import ---
# FairData import service expects: places.csv, events.csv, items.csv, taxonomies.csv
# PlainCsvIngester outputs per-model files. Combine them here.

def combine_csvs(output, source_names, target_name)
  all_headers = []
  tables = []

  source_names.each do |name|
    path = File.join(output, "#{name}.csv")
    next unless File.exist?(path)

    table = CSV.read(path, headers: true)
    table.headers.each { |h| all_headers << h unless all_headers.include?(h) }
    tables << table
    File.delete(path)
  end

  return if tables.empty?

  CSV.open(File.join(output, "#{target_name}.csv"), 'w') do |csv|
    csv << all_headers
    tables.each do |table|
      table.each do |row|
        csv << all_headers.map { |h| row[h] }
      end
    end
  end

  row_count = tables.sum(&:size)
  puts "Combined #{source_names.join(' + ')} → #{target_name}.csv (#{row_count} rows)"
end

# Combine Place models: settlements + colonial_landscape + countries + states_provinces → places.csv
combine_csvs(output, %w[settlements colonial_landscape countries states_provinces], 'places')

# Combine Taxonomy models → taxonomies.csv
combine_csvs(output, %w[group_types event_types location_accuracy landscape_types], 'taxonomies')

# Rename sources → items.csv (FairData expects items.csv for Item model)
sources_path = File.join(output, 'sources.csv')
if File.exist?(sources_path)
  FileUtils.mv(sources_path, File.join(output, 'items.csv'))
  puts "Renamed sources.csv → items.csv"
end

# events.csv — replace id_map UUIDs with fresh ones to avoid stale record
# matching. FairData's "clear data" is async and may leave ghost records;
# pre-assigned UUIDs match them, causing UPDATE to invisible records instead
# of INSERT. Fresh UUIDs force INSERT while staying consistent with
# relationships.csv (which is generated after this step using events_uuid_map).
events_path = File.join(output, 'events.csv')
events_uuid_map = {}  # old UUID -> new UUID, used by relationship generator
if File.exist?(events_path)
  table = CSV.read(events_path, headers: true)
  table.each do |row|
    old_uuid = row['uuid']
    new_uuid = SecureRandom.uuid
    events_uuid_map[old_uuid] = new_uuid if old_uuid && !old_uuid.empty?
    row['uuid'] = new_uuid
  end
  CSV.open(events_path, 'w') do |csv|
    csv << table.headers
    table.each { |row| csv << row }
  end
  puts "Replaced #{events_uuid_map.size} event UUIDs with fresh ones (ghost record workaround)"
end

puts "\nTransform and post-processing complete."
puts "Output files ready for FairData import in: #{output}"
# --- Relationship Generation ---

id_map_dir = File.expand_path('./id_maps/marronage', File.dirname(__FILE__))

def load_id_map(dir, model)
  path = File.join(dir, "#{model}_map.json")
  return {} unless File.exist?(path)
  JSON.parse(File.read(path))
end

# Load all id maps
settlements_map        = load_id_map(id_map_dir, 'settlements')
# Remap event UUIDs through the fresh UUID map so relationships.csv
# references the same UUIDs as the rewritten events.csv
events_map_raw         = load_id_map(id_map_dir, 'events')
events_map             = events_map_raw.transform_values { |old_uuid| events_uuid_map[old_uuid] || old_uuid }
colonial_landscape_map = load_id_map(id_map_dir, 'colonial_landscape')
group_types_map        = load_id_map(id_map_dir, 'group_types')
event_types_map        = load_id_map(id_map_dir, 'event_types')
location_accuracy_map  = load_id_map(id_map_dir, 'location_accuracy')
landscape_types_map    = load_id_map(id_map_dir, 'landscape_types')
sources_map            = load_id_map(id_map_dir, 'sources')
countries_map          = load_id_map(id_map_dir, 'countries')
states_provinces_map   = load_id_map(id_map_dir, 'states_provinces')

relationships = []
counts = Hash.new(0)

# 1. Settlement → Group Type
CSV.foreach(File.join(intermediate, 'settlements.csv'), headers: true) do |row|
  primary_uuid = settlements_map[row['id']]
  related_uuid = group_types_map[row['group_type']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_SETTLEMENTS_GROUP_TYPES'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Taxonomy'
  }
  counts[:settlements_group_types] += 1
end

# 2. Settlement → Location Accuracy
CSV.foreach(File.join(intermediate, 'settlements.csv'), headers: true) do |row|
  primary_uuid = settlements_map[row['id']]
  related_uuid = location_accuracy_map[row['location_accuracy']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_SETTLEMENTS_LOCATION_ACCURACY'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Taxonomy'
  }
  counts[:settlements_location_accuracy] += 1
end

# 3. Event → Settlement
CSV.foreach(File.join(intermediate, 'events.csv'), headers: true) do |row|
  primary_uuid = settlements_map[row['settlement_pid']]
  related_uuid = events_map[row['id']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_SETTLEMENTS_EVENTS'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Event'
  }
  counts[:settlements_events] += 1
end

# 4. Event → Start Event Type
CSV.foreach(File.join(intermediate, 'events.csv'), headers: true) do |row|
  primary_uuid = events_map[row['id']]
  related_uuid = event_types_map[row['start_event']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_EVENTS_START_EVENT_TYPE'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Event',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Taxonomy'
  }
  counts[:events_start_event_type] += 1
end

# 5. Event → Change Event Type
CSV.foreach(File.join(intermediate, 'events.csv'), headers: true) do |row|
  primary_uuid = events_map[row['id']]
  related_uuid = event_types_map[row['change_event']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_EVENTS_CHANGE_EVENT_TYPE'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Event',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Taxonomy'
  }
  counts[:events_change_event_type] += 1
end

# 6. Colonial Landscape → Landscape Type
CSV.foreach(File.join(intermediate, 'colonial_landscape.csv'), headers: true) do |row|
  primary_uuid = colonial_landscape_map[row['id']]
  related_uuid = landscape_types_map[row['type']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_COLONIAL_LANDSCAPE_TYPES'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Taxonomy'
  }
  counts[:colonial_landscape_types] += 1
end

# 7. Event → Sources (from NocoDB API export)
events_sources_file = File.join(input, 'events_sources.csv')
if File.exist?(events_sources_file)
  CSV.foreach(events_sources_file, headers: true) do |row|
    primary_uuid = events_map[row['pid_event']]
    related_uuid = sources_map[row['pid_source']]
    next if primary_uuid.nil? || related_uuid.nil?
    relationships << {
      project_model_relationship_id: env['REL_EVENTS_SOURCES'],
      uuid: SecureRandom.uuid,
      primary_record_uuid: primary_uuid,
      primary_record_type: 'CoreDataConnector::Event',
      related_record_uuid: related_uuid,
      related_record_type: 'CoreDataConnector::Item'
    }
    counts[:events_sources] += 1
  end
end

# 8. Settlement → Country
CSV.foreach(File.join(intermediate, 'settlements.csv'), headers: true) do |row|
  primary_uuid = settlements_map[row['id']]
  related_uuid = countries_map[row['present_location_country']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_SETTLEMENTS_COUNTRIES'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Place'
  }
  counts[:settlements_countries] += 1
end

# 9. Settlement → States/Provinces (multi-value, split and normalize)
CSV.foreach(File.join(intermediate, 'settlements.csv'), headers: true) do |row|
  primary_uuid = settlements_map[row['id']]
  next if primary_uuid.nil?
  raw = row['present_location_state_or_province']&.strip
  next if raw.nil? || raw.empty?
  raw.split(',').map(&:strip).each do |val|
    val = normalize_state(val)
    related_uuid = states_provinces_map[val]
    next if related_uuid.nil?
    relationships << {
      project_model_relationship_id: env['REL_SETTLEMENTS_STATES'],
      uuid: SecureRandom.uuid,
      primary_record_uuid: primary_uuid,
      primary_record_type: 'CoreDataConnector::Place',
      related_record_uuid: related_uuid,
      related_record_type: 'CoreDataConnector::Place'
    }
    counts[:settlements_states] += 1
  end
end

# 10. Colonial Landscape → Country
CSV.foreach(File.join(intermediate, 'colonial_landscape.csv'), headers: true) do |row|
  primary_uuid = colonial_landscape_map[row['id']]
  related_uuid = countries_map[row['present_location_country']]
  next if primary_uuid.nil? || related_uuid.nil?
  relationships << {
    project_model_relationship_id: env['REL_CL_COUNTRIES'],
    uuid: SecureRandom.uuid,
    primary_record_uuid: primary_uuid,
    primary_record_type: 'CoreDataConnector::Place',
    related_record_uuid: related_uuid,
    related_record_type: 'CoreDataConnector::Place'
  }
  counts[:cl_countries] += 1
end

# Write relationships.csv
rel_headers = %w[project_model_relationship_id uuid primary_record_uuid primary_record_type related_record_uuid related_record_type]
CSV.open(File.join(output, 'relationships.csv'), 'w') do |csv|
  csv << rel_headers
  relationships.each { |r| csv << rel_headers.map { |h| r[h.to_sym] } }
end

puts "Generated #{relationships.size} relationships → relationships.csv"
puts "  settlements → group_types:        #{counts[:settlements_group_types]}"
puts "  settlements → location_accuracy:  #{counts[:settlements_location_accuracy]}"
puts "  settlements → events:             #{counts[:settlements_events]}"
puts "  events → start_event_type:        #{counts[:events_start_event_type]}"
puts "  events → change_event_type:       #{counts[:events_change_event_type]}"
puts "  colonial_landscape → types:       #{counts[:colonial_landscape_types]}"
puts "  events → sources:                #{counts[:events_sources]}"
puts "  settlements → countries:         #{counts[:settlements_countries]}"
puts "  settlements → states:            #{counts[:settlements_states]}"
puts "  colonial_landscape → countries:  #{counts[:cl_countries]}"
puts "\nMigration complete. All output files in: #{output}"
