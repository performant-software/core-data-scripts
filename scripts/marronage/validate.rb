require 'csv'

output = File.expand_path('../../data/marronage/output', __FILE__)

puts "=== Marronage Migration Validation ==="
puts

# 1. Check all expected FairData import files exist
expected_files = %w[places.csv events.csv items.csv taxonomies.csv relationships.csv]
puts "Checking for FairData import files:"
expected_files.each do |f|
  path = File.join(output, f)
  if File.exist?(path)
    rows = CSV.read(path, headers: true).size
    puts "  OK  #{f}: #{rows} rows"
  else
    puts "  MISSING  #{f}"
  end
end

puts

# 2. Check for PLACEHOLDER values
puts "Checking for PLACEHOLDER values:"
placeholder_found = false
Dir.glob(File.join(output, '*.csv')).each do |f|
  content = File.read(f)
  if content.include?('PLACEHOLDER')
    puts "  WARNING  #{File.basename(f)} contains PLACEHOLDER values"
    placeholder_found = true
  end
end
puts "  OK  No PLACEHOLDER values found" unless placeholder_found
puts "  (PLACEHOLDERs are expected until FairData project is created)" if placeholder_found

puts

# 3. Validate places.csv
places_path = File.join(output, 'places.csv')
if File.exist?(places_path)
  places = CSV.read(places_path, headers: true)
  no_name = places.count { |r| r['name'].nil? || r['name'].strip.empty? }
  no_coords = places.count { |r| r['latitude'].nil? || r['latitude'].strip.empty? }
  model_ids = places.map { |r| r['project_model_id'] }.uniq

  puts "Places:"
  puts "  Total: #{places.size}"
  puts "  Missing name: #{no_name}"
  puts "  Missing coordinates: #{no_coords} (some settlements have n/a coords — expected)"
  puts "  Distinct project_model_ids: #{model_ids.join(', ')}"
  puts "  (expect 2: one for settlements, one for colonial landscape)" unless model_ids.size == 2
end

puts

# 4. Validate events.csv
events_path = File.join(output, 'events.csv')
if File.exist?(events_path)
  events = CSV.read(events_path, headers: true)
  no_date = events.count { |r| r['start_date'].nil? || r['start_date'].strip.empty? }
  puts "Events:"
  puts "  Total: #{events.size}"
  puts "  Missing start_date: #{no_date}"
end

puts

# 5. Validate items.csv (sources)
items_path = File.join(output, 'items.csv')
if File.exist?(items_path)
  items = CSV.read(items_path, headers: true)
  puts "Items (Sources):"
  puts "  Total: #{items.size}"
end

puts

# 6. Validate taxonomies.csv
taxonomies_path = File.join(output, 'taxonomies.csv')
if File.exist?(taxonomies_path)
  taxonomies = CSV.read(taxonomies_path, headers: true)
  model_ids = taxonomies.map { |r| r['project_model_id'] }.uniq
  puts "Taxonomies:"
  puts "  Total: #{taxonomies.size}"
  puts "  Distinct project_model_ids: #{model_ids.join(', ')}"
  puts "  (expect 4: group_types, event_types, location_accuracy, landscape_types)" unless model_ids.size == 4
end

puts

# 7. Validate relationships.csv
rels_path = File.join(output, 'relationships.csv')
if File.exist?(rels_path)
  rels = CSV.read(rels_path, headers: true)
  by_type = rels.group_by { |r| r['project_model_relationship_id'] }

  missing_primary = rels.count { |r| r['primary_record_uuid'].nil? || r['primary_record_uuid'].strip.empty? }
  missing_related = rels.count { |r| r['related_record_uuid'].nil? || r['related_record_uuid'].strip.empty? }

  puts "Relationships:"
  puts "  Total: #{rels.size}"
  by_type.each do |type_id, rows|
    puts "  Relationship ID #{type_id}: #{rows.size} rows"
  end
  puts "  WARNING: #{missing_primary} rows missing primary_record_uuid" if missing_primary > 0
  puts "  WARNING: #{missing_related} rows missing related_record_uuid" if missing_related > 0
end

puts
puts "=== Validation complete ==="
