#!/usr/bin/env ruby
# csv_import.rb — Transform RelNet nodegoat exports into FairData import CSVs.
#
# Usage:
#   ruby scripts/relnet/csv_import.rb [staging]
#
# Reads the four nodegoat exports from data/relnet/input/ (copy them there from
# ~/repos/relnet/nodegoat-export/), pre-processes each into clean singular-row
# intermediate CSVs, runs the shared PlainCsvIngester to assign stable UUIDs and
# build FairData-shaped CSVs, then post-processes (combine person/taxonomy files,
# enrich place uncertainty, generate relationships.csv).
#
# Modeled on scripts/marronage/csv_import.rb. See relnet/fairdata-model.md and
# relnet/docs/data-mapping.md for the field mappings and decisions.
#
# REQUIRES: .env.staging populated with real IDs (run setup_project.rb first).

require 'csv'
require 'json'
require 'securerandom'
require 'fileutils'
require 'dotenv'
require_relative '../../core/csv/plain_csv_ingester'
require_relative '../../core/geo'
require_relative 'enrich_certainty_radius'

env = Dotenv.parse(File.expand_path("./.env.#{ARGV[0] || 'staging'}", __dir__))

input        = File.expand_path('data/relnet/input')
output       = File.expand_path('data/relnet/output')
intermediate = File.expand_path('data/relnet/intermediate')
FileUtils.mkdir_p(intermediate)
FileUtils.mkdir_p(output)

PLEIADES_PREFIX = %r{\Ahttps?://pleiades\.stoa\.org/places/}.freeze

# --- helpers ---------------------------------------------------------------

# Geometry parsing (GeoJSON [lon,lat] -> [lat,lon]; Point/Polygon/GeometryCollection)
# lives in core/geo.rb — use Geo.parse_point.

def normalize_pleiades(val)
  return nil if val.nil? || val.strip.empty?
  val.strip.sub(PLEIADES_PREFIX, '')
end

# Collapse the multi-row nodegoat export into one row per Object ID, collecting
# the multi-value `Alternative Names` column into a comma-joined distinct string.
# Returns { object_id => { 'first row values' plus :alt_names } }.
def collapse(path, alt_col: 'Alternative Names', id_col: 'Object ID')
  by_id = {}
  CSV.foreach(path, headers: true, encoding: 'bom|utf-8') do |row|
    oid = row[id_col]&.strip
    next if oid.nil? || oid.empty?
    rec = (by_id[oid] ||= { row: row, alts: [] })
    alt = row[alt_col]&.strip
    rec[:alts] << alt if alt && !alt.empty? && !rec[:alts].include?(alt)
  end
  by_id
end

def add_unique(list, val)
  v = val&.strip
  list << v if v && !v.empty? && !list.include?(v)
end

# --- 1. Places -------------------------------------------------------------
def prepare_places(input, intermediate)
  src = File.join(input, 'places.csv')
  raise "places.csv not found in #{input}" unless File.exist?(src)

  place_types = []
  by_id = collapse(src)

  cols = %w[id name latitude longitude alt_names description pleiades_id additional_urls kind_of_place part_of_ref nodegoat_id]
  CSV.open(File.join(intermediate, 'places.csv'), 'w', headers: cols, write_headers: true) do |csv|
    by_id.each do |oid, rec|
      row = rec[:row]
      lat, lon = Geo.parse_point(row['[Location] Geometry'])
      kind = row['Kind of Place']&.strip
      add_unique(place_types, kind)
      csv << [
        oid, row['Name'], lat, lon,
        rec[:alts].join(', '),
        row['Description'],
        normalize_pleiades(row['Pleiades URI']),
        row['Additional URLs'],
        kind,
        row['[Location] Location Reference']&.strip,
        oid
      ]
    end
  end

  write_taxonomy(intermediate, 'place_types', place_types)
  puts "Wrote intermediate/places.csv (#{by_id.size} places, #{place_types.size} place types)"
end

# --- 2. Tablets (+ Writing Classifications) --------------------------------
# Prefers tablets_with_relations.csv (the Phase-B export carrying place/divine/
# cultic cross-references). It repeats the singular tablet columns 0-22 with the
# SAME header names, so Ruby CSV's row['<name>'] returns the first (tablet)
# occurrence — exactly the singular fields. The trailing cross-ref triples
# (duplicate 'Object ID'/'Name' headers) are read positionally in the
# relationship step, not here. Falls back to the singular-only tablets.csv.
def prepare_tablets(input, intermediate)
  src = ['tablets_with_relations.csv', 'tablets.csv'].map { |n| File.join(input, n) }.find { |p| File.exist?(p) }
  raise "no tablets export found in #{input}" unless src
  puts "  (tablets source: #{File.basename(src)})"

  writing_values = []
  by_id = collapse(src, alt_col: 'Museum Collection') # the dup-row source here is Museum Collection

  cols = %w[id name relnet_id text_no publication_no joined_tablet museum_name museum_object_id
            museum_collection colophons rubrics measurements summary notes commentary
            commentary_tablet date date_gregorian checked provenance writing nodegoat_id]

  CSV.open(File.join(intermediate, 'tablets.csv'), 'w', headers: cols, write_headers: true) do |csv|
    by_id.each do |oid, rec|
      row = rec[:row]
      museum_oid = row['Museum - Object ID']&.strip
      museum_name = row['Museum']&.strip

      writing = row['Writing']&.strip
      add_unique(writing_values, writing)

      checked = case row['Checked']&.strip
                when 'Yes' then 'true'
                when 'No'  then 'false'
                else nil
                end

      csv << [
        oid, row['Name'], row['RelNet ID'], row['Text No.'], row['Publication No.'],
        row['Joined Tablet'], museum_name, museum_oid, rec[:alts].join(', '),
        row['Colophons'], row['Rubrics'], row['Measurements'], row['Summary of the tablet'],
        row['Notes'], row['Commentary'], row['Commentary to Tablet'],
        row['Date'],
        nil,                       # date_gregorian — TODO: derive from regnal date (needs reign->year table)
        checked, row['Provenance'], writing, oid
      ]
    end
  end

  write_taxonomy(intermediate, 'writing_classifications', writing_values)
  puts "Wrote intermediate/tablets.csv (#{by_id.size} tablets), #{writing_values.size} writing classes"
end

# --- 2b. Museums (Organizations) -------------------------------------------
# Prefers museums_with_tablets.csv, which carries the museum URL (cols: museum
# nodegoat ID, Object ID, Name, source×3, Name, URL, then a tablet triple).
# One row per museum↔tablet pair, so dedup by museum Object ID. Falls back to
# extracting name-only museums from the tablets export.
def prepare_museums(input, intermediate)
  src = File.join(input, 'museums_with_tablets.csv')
  museums = {} # object_id => { name:, url: }
  if File.exist?(src)
    CSV.foreach(src, headers: true, encoding: 'bom|utf-8') do |row|
      oid = row[1]&.strip                       # museum Object ID (first 'Object ID' col)
      next if oid.nil? || oid.empty?
      museums[oid] ||= { name: row[2]&.strip, url: row['URL']&.strip }
    end
  else
    # Fallback: derive from the tablets export (name only).
    tsrc = ['tablets_with_relations.csv', 'tablets.csv'].map { |n| File.join(input, n) }.find { |p| File.exist?(p) }
    CSV.foreach(tsrc, headers: true, encoding: 'bom|utf-8') do |row|
      oid = row['Museum - Object ID']&.strip
      next if oid.nil? || oid.empty?
      museums[oid] ||= { name: row['Museum']&.strip, url: nil }
    end
  end

  CSV.open(File.join(intermediate, 'museums.csv'), 'w', headers: %w[id name url nodegoat_id], write_headers: true) do |csv|
    museums.each { |oid, m| csv << [oid, m[:name], m[:url], oid] }
  end
  puts "Wrote intermediate/museums.csv (#{museums.size} museums, #{museums.count { |_, m| m[:url] && !m[:url].empty? }} with URL)"
end

# --- 3. Cultic Actors ------------------------------------------------------
def prepare_cultic_actors(input, intermediate, genders)
  src = File.join(input, 'cultic-actor.csv')
  raise "cultic-actor.csv not found in #{input}" unless File.exist?(src)

  by_id = collapse(src)
  cols = %w[id last_name alt_names gender nodegoat_id]
  CSV.open(File.join(intermediate, 'cultic_actors.csv'), 'w', headers: cols, write_headers: true) do |csv|
    by_id.each do |oid, rec|
      row = rec[:row]
      g = row['Gender']&.strip
      add_unique(genders, g)
      csv << [oid, row['Name'], rec[:alts].join(', '), g, oid]
    end
  end
  puts "Wrote intermediate/cultic_actors.csv (#{by_id.size} actors)"
end

# --- 4. Divine Characters --------------------------------------------------
def prepare_divine_characters(input, intermediate, genders)
  src = File.join(input, 'divine-character.csv')
  raise "divine-character.csv not found in #{input}" unless File.exist?(src)

  capacities = []
  by_id = collapse(src)
  cols = %w[id last_name alt_names function gender capacity alt_name_for_object_id nodegoat_id]
  CSV.open(File.join(intermediate, 'divine_characters.csv'), 'w', headers: cols, write_headers: true) do |csv|
    by_id.each do |oid, rec|
      row = rec[:row]
      g = row['Gender']&.strip
      cap = row['Capacity']&.strip
      add_unique(genders, g)
      add_unique(capacities, cap)
      csv << [
        oid, row['Name'], rec[:alts].join(', '), row['Function'],
        g, cap, row['Alternative Name for - Object ID']&.strip, oid
      ]
    end
  end

  write_taxonomy(intermediate, 'divine_capacities', capacities)
  puts "Wrote intermediate/divine_characters.csv (#{by_id.size} deities, #{capacities.size} capacities)"
end

# A taxonomy intermediate is just { id == name }.
def write_taxonomy(intermediate, name, values)
  CSV.open(File.join(intermediate, "#{name}.csv"), 'w', headers: %w[id name], write_headers: true) do |csv|
    values.each { |v| csv << [v, v] }
  end
end

# --- Run pre-processing ----------------------------------------------------
genders = []
prepare_places(input, intermediate)
prepare_tablets(input, intermediate)
prepare_museums(input, intermediate)
prepare_cultic_actors(input, intermediate, genders)
prepare_divine_characters(input, intermediate, genders)
write_taxonomy(intermediate, 'genders', genders)
puts "Pre-processing complete (#{genders.size} gender values). Intermediate: #{intermediate}\n\n"

# --- FairData CSV transform via PlainCsvIngester ---------------------------
udf = ->(key) { "udf_#{env.fetch(key).tr('-', '_')}".to_sym }

fields = {
  places: {
    'name': 'name', 'latitude': 'latitude', 'longitude': 'longitude',
    udf.('UDF_PLACES_ALT_NAMES') => 'alt_names',
    udf.('UDF_PLACES_DESCRIPTION') => 'description',
    udf.('UDF_PLACES_PLEIADES_ID') => 'pleiades_id',
    udf.('UDF_PLACES_ADDITIONAL_URLS') => 'additional_urls',
    udf.('UDF_PLACES_NODEGOAT_ID') => 'nodegoat_id'
  },
  tablets: {
    'name': 'name',
    udf.('UDF_TABLETS_RELNET_ID') => 'relnet_id',
    udf.('UDF_TABLETS_TEXT_NO') => 'text_no',
    udf.('UDF_TABLETS_PUBLICATION_NO') => 'publication_no',
    udf.('UDF_TABLETS_JOINED_TABLET') => 'joined_tablet',
    udf.('UDF_TABLETS_MUSEUM_COLLECTION') => 'museum_collection',
    udf.('UDF_TABLETS_COLOPHONS') => 'colophons',
    udf.('UDF_TABLETS_RUBRICS') => 'rubrics',
    udf.('UDF_TABLETS_MEASUREMENTS') => 'measurements',
    udf.('UDF_TABLETS_SUMMARY') => 'summary',
    udf.('UDF_TABLETS_NOTES') => 'notes',
    udf.('UDF_TABLETS_COMMENTARY') => 'commentary',
    udf.('UDF_TABLETS_COMMENTARY_TABLET') => 'commentary_tablet',
    udf.('UDF_TABLETS_DATE') => 'date',
    udf.('UDF_TABLETS_DATE_GREGORIAN') => 'date_gregorian',
    udf.('UDF_TABLETS_CHECKED') => 'checked',
    udf.('UDF_TABLETS_NODEGOAT_ID') => 'nodegoat_id'
  },
  museums: {
    'name': 'name',
    'description': '_empty', # native column required by importer COPY order (Organization: name, description)
    udf.('UDF_MUSEUMS_URL') => 'url',
    udf.('UDF_MUSEUMS_NODEGOAT_ID') => 'nodegoat_id'
  },
  cultic_actors: {
    # Native Person columns in the importer's COPY order: last_name, first_name, middle_name, biography.
    # Ancient names aren't first/last, so the whole name goes in last_name; the rest are present-but-empty.
    'last_name': 'last_name',
    'first_name': '_empty',
    'middle_name': '_empty',
    'biography': '_empty',
    udf.('UDF_CULTIC_ALT_NAMES') => 'alt_names',
    udf.('UDF_CULTIC_NODEGOAT_ID') => 'nodegoat_id'
  },
  divine_characters: {
    'last_name': 'last_name',
    'first_name': '_empty',
    'middle_name': '_empty',
    'biography': '_empty',
    udf.('UDF_DIVINE_ALT_NAMES') => 'alt_names',
    udf.('UDF_DIVINE_FUNCTION') => 'function',
    udf.('UDF_DIVINE_NODEGOAT_ID') => 'nodegoat_id'
  },
  place_types: { 'name': 'name' },
  writing_classifications: { 'name': 'name' },
  genders: { 'name': 'name' },
  divine_capacities: { 'name': 'name' }
}

model_files = %w[places tablets museums cultic_actors divine_characters
                 place_types writing_classifications genders divine_capacities]

transform = Csv::PlainCsvIngester.new(
  input: intermediate,
  output: output,
  id_map_path: File.expand_path('./id_maps', File.dirname(__FILE__)),
  env: env,
  fields: fields,
  model_files: model_files
)
transform.parse_models
transform.cleanup(model_files, ['original_id'])
puts "PlainCsvIngester transform complete.\n\n"

# --- Post-processing: combine into FairData import files -------------------
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
    tables.each { |t| t.each { |row| csv << all_headers.map { |h| row[h] } } }
  end
  puts "Combined #{source_names.join(' + ')} -> #{target_name}.csv (#{tables.sum(&:size)} rows)"
end

# Two Person models share people.csv (different project_model_id).
combine_csvs(output, %w[cultic_actors divine_characters], 'people')
# Four Taxonomy models share taxonomies.csv.
combine_csvs(output, %w[place_types writing_classifications genders divine_capacities], 'taxonomies')
# Item -> items.csv, Organization -> organizations.csv
FileUtils.mv(File.join(output, 'tablets.csv'), File.join(output, 'items.csv')) if File.exist?(File.join(output, 'tablets.csv'))
FileUtils.mv(File.join(output, 'museums.csv'), File.join(output, 'organizations.csv')) if File.exist?(File.join(output, 'museums.csv'))

# Place uncertainty: add `properties` { certainty_radius } in km (meters/1000).
# DISABLED for staging: the connector version on staging.coredata.cloud does NOT
# support a Places `properties` column (added in core-data-connector PR #197 but
# not yet deployed to staging). Including the column breaks the COPY column count.
# Re-enable once staging is updated; the scaffold + override mechanism are ready.
if ENV['RELNET_ENABLE_PROPERTIES'] == '1'
  RelnetCertaintyRadius.enrich(
    places_path: File.join(output, 'places.csv'),
    overrides_path: File.join(input, 'certainty_radius.csv'),
    env: env
  )
else
  puts "Skipping certainty_radius enrichment (staging connector lacks `properties` support)."
end

puts "\nTransform complete. FairData import files in: #{output}\n\n"

# --- Relationship generation ----------------------------------------------
id_map_dir = File.expand_path('./id_maps', File.dirname(__FILE__))
def load_map(dir, model)
  path = File.join(dir, "#{model}_map.json")
  File.exist?(path) ? JSON.parse(File.read(path)) : {}
end

places_map      = load_map(id_map_dir, 'places')
tablets_map     = load_map(id_map_dir, 'tablets')
museums_map     = load_map(id_map_dir, 'museums')
cultic_map      = load_map(id_map_dir, 'cultic_actors')
divine_map      = load_map(id_map_dir, 'divine_characters')
place_types_map = load_map(id_map_dir, 'place_types')
writing_map     = load_map(id_map_dir, 'writing_classifications')
genders_map     = load_map(id_map_dir, 'genders')
capacities_map  = load_map(id_map_dir, 'divine_capacities')

# Name -> place object id, for resolving Part-of refs by name.
# nodegoat place names embed their kind as a suffix, e.g. "Babylon (City)".
# Provenance values are bare city names ("Babylon"), so we ALSO index City-type
# places by their stem (name minus the " (City)" suffix) for Findspot matching.
place_id_by_name = {}
city_id_by_stem  = {}
CSV.foreach(File.join(intermediate, 'places.csv'), headers: true) do |row|
  name = row['name']&.strip
  place_id_by_name[name] = row['id']
  if row['kind_of_place']&.strip == 'City' && name
    stem = name.sub(/\s*\(City\)\s*\z/, '')
    city_id_by_stem[stem] = row['id']
  end
end

PLACE = 'CoreDataConnector::Place'
ITEM  = 'CoreDataConnector::Item'
ORG   = 'CoreDataConnector::Organization'
PERSON = 'CoreDataConnector::Person'
TAX   = 'CoreDataConnector::Taxonomy'

rels = []
counts = Hash.new(0)
def rel(rels, counts, key, rel_id, p_uuid, p_type, r_uuid, r_type)
  return if rel_id.nil? || rel_id == 'PLACEHOLDER' || p_uuid.nil? || r_uuid.nil?
  rels << { project_model_relationship_id: rel_id, uuid: SecureRandom.uuid,
            primary_record_uuid: p_uuid, primary_record_type: p_type,
            related_record_uuid: r_uuid, related_record_type: r_type }
  counts[key] += 1
end

# Tablets: Held at (museum), Findspot (provenance), Writing
CSV.foreach(File.join(intermediate, 'tablets.csv'), headers: true) do |row|
  t_uuid = tablets_map[row['id']]
  rel(rels, counts, :held_at, env['REL_TABLET_MUSEUM'], t_uuid, ITEM, museums_map[row['museum_object_id']], ORG)

  prov = row['provenance']&.strip
  if prov && !prov.empty? && prov.casecmp('Unknown') != 0
    place_oid = place_id_by_name[prov] || city_id_by_stem[prov]
    if place_oid
      rel(rels, counts, :findspot, env['REL_TABLET_FINDSPOT'], t_uuid, ITEM, places_map[place_oid], PLACE)
    else
      warn "  ! Findspot: no Place matched provenance #{prov.inspect}"
    end
  end

  rel(rels, counts, :writing, env['REL_TABLET_WRITING'], t_uuid, ITEM, writing_map[row['writing']&.strip], TAX)
end

# Places: Part of (containment), Kind of Place
CSV.foreach(File.join(intermediate, 'places.csv'), headers: true) do |row|
  p_uuid = places_map[row['id']]
  ref = row['part_of_ref']&.strip
  if ref && !ref.empty?
    parent_oid = places_map.key?(ref) ? ref : place_id_by_name[ref]  # ref may be an Object ID or a name
    rel(rels, counts, :part_of, env['REL_PLACE_PARTOF'], p_uuid, PLACE, places_map[parent_oid], PLACE) if parent_oid
  end
  rel(rels, counts, :kind, env['REL_PLACE_KIND'], p_uuid, PLACE, place_types_map[row['kind_of_place']&.strip], TAX)
end

# Cultic Actors: Gender
CSV.foreach(File.join(intermediate, 'cultic_actors.csv'), headers: true) do |row|
  rel(rels, counts, :cultic_gender, env['REL_CULTIC_GENDER'], cultic_map[row['id']], PERSON, genders_map[row['gender']&.strip], TAX)
end

# Divine Characters: Gender, Capacity, Alternative name for (self-ref)
CSV.foreach(File.join(intermediate, 'divine_characters.csv'), headers: true) do |row|
  d_uuid = divine_map[row['id']]
  rel(rels, counts, :divine_gender, env['REL_DIVINE_GENDER'], d_uuid, PERSON, genders_map[row['gender']&.strip], TAX)
  rel(rels, counts, :divine_capacity, env['REL_DIVINE_CAPACITY'], d_uuid, PERSON, capacities_map[row['capacity']&.strip], TAX)
  alt_for = row['alt_name_for_object_id']&.strip
  rel(rels, counts, :alt_name_for, env['REL_DIVINE_ALT_NAME_FOR'], d_uuid, PERSON, divine_map[alt_for], PERSON) if alt_for && !alt_for.empty?
end

# Tablet cross-references: Places / Divine Characters / Cultic Actors "mentioned".
# Source: tablets_with_relations.csv. nodegoat emits a CARTESIAN PRODUCT when all
# three cross-ref fields are exported together, so we project the column triples
# and DEDUPE to distinct (tablet, related) pairs before emitting relationships.
# Columns (32-col layout): tablet Object ID = 1; place = 24; divine = 27; cultic = 30.
# (Going forward Rocío will export one cross-ref per file, which would replace this
#  block with three small per-relation readers — same pair-sets, no dedup needed.)
rel_src = File.join(input, 'tablets_with_relations.csv')
if File.exist?(rel_src)
  seen = { place: {}, divine: {}, cultic: {} } # use hashes as ordered sets
  CSV.foreach(rel_src, headers: true, encoding: 'bom|utf-8') do |row|
    t = row[1]&.strip
    next if t.nil? || t.empty?
    { place: 24, divine: 27, cultic: 30 }.each do |k, idx|
      v = row[idx]&.strip
      seen[k]["#{t}\t#{v}"] = true if v && !v.empty?
    end
  end
  pair = ->(k) { seen[k].keys.map { |s| s.split("\t", 2) } }
  unmatched = Hash.new(0)
  pair.(:place).each do |t, p|
    tu = tablets_map[t]; ru = places_map[p]
    ru ? rel(rels, counts, :places_mentioned, env['REL_TABLET_PLACES_MENTIONED'], tu, ITEM, ru, PLACE) : unmatched[:place] += 1
  end
  pair.(:divine).each do |t, d|
    tu = tablets_map[t]; ru = divine_map[d]
    ru ? rel(rels, counts, :divine_mentioned, env['REL_TABLET_DIVINE_MENTIONED'], tu, ITEM, ru, PERSON) : unmatched[:divine] += 1
  end
  pair.(:cultic).each do |t, c|
    tu = tablets_map[t]; ru = cultic_map[c]
    ru ? rel(rels, counts, :cultic_mentioned, env['REL_TABLET_CULTIC_MENTIONED'], tu, ITEM, ru, PERSON) : unmatched[:cultic] += 1
  end
  puts "  cross-ref unmatched (related record not yet imported): #{unmatched.map { |k, v| "#{k}=#{v}" }.join(' ')}" unless unmatched.empty?
end

headers = %w[project_model_relationship_id uuid primary_record_uuid primary_record_type related_record_uuid related_record_type]
CSV.open(File.join(output, 'relationships.csv'), 'w') do |csv|
  csv << headers
  rels.each { |r| csv << headers.map { |h| r[h.to_sym] } }
end

puts "Generated #{rels.size} relationships -> relationships.csv"
counts.each { |k, v| puts format('  %-18s %d', k, v) }
puts "\nNote: Phase B 'mentioned' cross-references (Tablet->Place/Deity/Actor) await the"
puts "nodegoat tablet multi-value exports; their relationship definitions already exist."
puts "Note: date_gregorian is left empty pending a regnal-date -> Gregorian-year derivation."
puts "\nMigration complete. Output: #{output}"
