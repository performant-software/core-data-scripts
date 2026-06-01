#!/usr/bin/env ruby
# frozen_string_literal: true

# Enrich places.csv with a `properties` JSON column containing `certainty_radius`
# (km, rounded to 0.01) derived from activity-site circle polygons in the
# Marronage geojson. One polygon per event; multiple events may share a
# settlement — we keep the max radius per settlement.
#
# Standalone usage:
#   bundle exec ruby scripts/marronage/enrich_certainty_radius.rb
#
# Library usage (from csv_import.rb):
#   require_relative 'enrich_certainty_radius'
#   MarronageCertaintyRadius.enrich(
#     places_path: '/path/to/places.csv',
#     geojson_path: '/path/to/activity_site_polygons_events_04.geojson',
#     env: ENV_HASH
#   )

require 'csv'
require 'json'
require 'optparse'
require_relative '../../core/geo'

module MarronageCertaintyRadius
  module_function

  # Minimal stdlib .env parser — avoids the dotenv gem so the script works
  # under system Ruby without `bundle exec`. Handles `KEY=VALUE`, skips
  # comments and blank lines. Values are unquoted as-is (no shell escaping).
  def parse_env_file(path)
    raise "env file not found: #{path}" unless File.exist?(path)

    {}.tap do |env|
      File.foreach(path) do |line|
        line = line.strip
        next if line.empty? || line.start_with?('#')

        key, _, value = line.partition('=')
        key = key.strip
        value = value.strip
        next if key.empty?

        # Strip surrounding quotes if present
        if (value.start_with?('"') && value.end_with?('"')) ||
           (value.start_with?("'") && value.end_with?("'"))
          value = value[1..-2]
        end

        env[key] = value
      end
    end
  end

  # haversine + circle-polygon radius math live in core/geo.rb (Geo.polygon_radius_km).

  # Build { settlement_id => max_radius_km (rounded to 0.01) } from a geojson path.
  def radius_map_from_geojson(geojson_path)
    fc = JSON.parse(File.read(geojson_path))
    features = fc['features'] || []

    raw = Hash.new { |h, k| h[k] = [] }
    unmatched = 0

    features.each do |feat|
      props = feat['properties'] || {}
      settlement_id = props['pid_town/activity']
      geom = feat['geometry']

      if settlement_id.nil? || settlement_id.to_s.empty?
        unmatched += 1
        next
      end

      next unless geom && geom['type'] == 'Polygon'

      ring = geom.dig('coordinates', 0)
      next if ring.nil? || ring.empty?

      raw[settlement_id] << Geo.polygon_radius_km(ring)
    end

    map = raw.transform_values { |rs| (rs.max * 100).round / 100.0 }
    [map, features.length, unmatched]
  end

  def enrich(places_path:, geojson_path:, env:, dry_run: false)
    unless File.exist?(places_path)
      raise "places.csv not found: #{places_path}"
    end
    unless File.exist?(geojson_path)
      raise "geojson not found: #{geojson_path}"
    end

    pid_uuid = env['UDF_SETTLEMENTS_PID']
    raise 'UDF_SETTLEMENTS_PID not set in env' if pid_uuid.nil? || pid_uuid.empty?

    settlements_model_id = env['PROJECT_MODEL_ID_SETTLEMENTS']
    raise 'PROJECT_MODEL_ID_SETTLEMENTS not set in env' if settlements_model_id.nil? || settlements_model_id.empty?
    settlements_model_id = settlements_model_id.to_i

    pid_column = "udf_#{pid_uuid.tr('-', '_')}"

    radius_map, feature_count, skipped = radius_map_from_geojson(geojson_path)

    table = CSV.read(places_path, headers: true)
    headers = table.headers.dup

    unless headers.include?(pid_column)
      raise "Settlements PID column #{pid_column.inspect} not found in #{places_path}. " \
            "Headers: #{headers.inspect}"
    end

    # Place `properties` immediately after `longitude` to match the import
    # service's expected column order (project_model_id, uuid, name, latitude,
    # longitude, properties, [UDF columns...]).
    headers.delete('properties')
    lon_idx = headers.index('longitude')
    if lon_idx
      headers.insert(lon_idx + 1, 'properties')
    else
      headers << 'properties'
    end

    matched = 0
    unmatched = []
    non_settlement = 0
    settlement_rows = 0

    table.each do |row|
      if row['project_model_id'].to_i == settlements_model_id
        settlement_rows += 1
        pid = row[pid_column]
        if pid && (radius = radius_map[pid])
          row['properties'] = JSON.generate({ certainty_radius: radius })
          matched += 1
        else
          row['properties'] = nil
          unmatched << pid if pid
        end
      else
        row['properties'] = nil
        non_settlement += 1
      end
    end

    puts '== GeoJSON =='
    puts "  file: #{geojson_path}"
    puts "  features: #{feature_count} (#{skipped} skipped — missing pid_town/activity)"
    puts "  unique settlements covered: #{radius_map.length}"
    unless radius_map.empty?
      values = radius_map.values
      puts format('  radius km: min=%.2f  max=%.2f  mean=%.2f',
                  values.min, values.max, values.sum / values.length.to_f)
    end

    puts '== places.csv =='
    puts "  file: #{places_path}"
    puts "  total rows: #{table.length}"
    puts "  settlement rows: #{settlement_rows}"
    puts "  non-settlement rows (properties left empty): #{non_settlement}"
    puts "  settlement rows enriched: #{matched}"
    puts "  settlement rows unmatched: #{unmatched.length}"
    unless unmatched.empty?
      sample = unmatched.first(5).join(', ')
      puts "  unmatched sample (first 5): #{sample}"
    end

    if dry_run
      puts '(dry-run) places.csv not written.'
      return { matched: matched, unmatched: unmatched.length, radius_map: radius_map }
    end

    CSV.open(places_path, 'w') do |csv|
      csv << headers
      table.each do |row|
        csv << headers.map { |h| row[h] }
      end
    end

    puts "Wrote enriched places.csv (#{headers.length} columns)."
    { matched: matched, unmatched: unmatched.length, radius_map: radius_map }
  end
end

if __FILE__ == $PROGRAM_NAME
  script_dir = File.dirname(File.expand_path(__FILE__))
  repo_root  = File.expand_path('../..', script_dir)

  options = {
    geojson: File.join(repo_root, 'data/marronage/input/activity_site_polygons_events_04.geojson'),
    places:  File.join(repo_root, 'data/marronage/output/places.csv'),
    env:     File.join(script_dir, '.env.development'),
    dry_run: false
  }

  OptionParser.new do |opts|
    opts.banner = 'Usage: enrich_certainty_radius.rb [options]'
    opts.on('--geojson PATH', 'Path to activity-site polygons geojson') { |v| options[:geojson] = v }
    opts.on('--places PATH',  'Path to places.csv to enrich')           { |v| options[:places]  = v }
    opts.on('--env PATH',     'Path to .env file')                      { |v| options[:env]     = v }
    opts.on('--dry-run',      'Report only; do not write places.csv')   { options[:dry_run] = true }
  end.parse!

  env = MarronageCertaintyRadius.parse_env_file(options[:env])

  MarronageCertaintyRadius.enrich(
    places_path: options[:places],
    geojson_path: options[:geojson],
    env: env,
    dry_run: options[:dry_run]
  )
end
