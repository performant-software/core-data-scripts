#!/usr/bin/env ruby
# frozen_string_literal: true
#
# enrich_certainty_radius.rb — add a `properties` JSON column carrying
# { "certainty_radius": <km> } to RelNet places.csv.
#
# RelNet expresses positional uncertainty at the building / sub-temple scale
# (e.g. "the well is somewhere inside Esagila"), so radii are specified in
# METERS. CDP / Peripleo consume `certainty_radius` in KILOMETERS, so we convert
# (meters / 1000) and keep 5 decimal places — fine enough for a 5 m radius
# (= 0.005 km). This is the meter-scale generalization of the km-scale mechanic
# Marronage used (decision D2, May 18).
#
# Source of radii: an optional override file at data/relnet/input/certainty_radius.csv
# with columns: nodegoat_object_id, radius_m
# (e.g. the Ziggurat of Babylon and any sub-temple features Rocío flags). If the
# file is absent, the `properties` column is still added (empty) in the correct
# position so the FairData importer's column order is satisfied.
#
# The `properties` column MUST sit immediately after `longitude` — the import
# service maps COPY columns by position (project_model_id, uuid, name, latitude,
# longitude, properties, [udf...]).

require 'csv'
require 'json'

module RelnetCertaintyRadius
  module_function

  def load_overrides(path)
    return {} unless path && File.exist?(path)
    map = {}
    CSV.foreach(path, headers: true, encoding: 'bom|utf-8') do |row|
      oid = row['nodegoat_object_id']&.strip
      m   = row['radius_m']
      next if oid.nil? || oid.empty? || m.nil? || m.strip.empty?
      map[oid] = (m.to_f / 1000.0).round(5) # meters -> km
    end
    map
  end

  def enrich(places_path:, overrides_path:, env:)
    raise "places.csv not found: #{places_path}" unless File.exist?(places_path)

    pid_uuid = env['UDF_PLACES_NODEGOAT_ID']
    raise 'UDF_PLACES_NODEGOAT_ID not set' if pid_uuid.nil? || pid_uuid.empty? || pid_uuid == 'PLACEHOLDER'
    pid_column = "udf_#{pid_uuid.tr('-', '_')}"

    radii = load_overrides(overrides_path)

    table = CSV.read(places_path, headers: true)
    headers = table.headers.dup
    headers.delete('properties')
    lon_idx = headers.index('longitude')
    lon_idx ? headers.insert(lon_idx + 1, 'properties') : headers << 'properties'

    unless table.headers.include?(pid_column)
      warn "  ! #{pid_column} not in places.csv — cannot match overrides; properties left empty."
      radii = {}
    end

    matched = 0
    table.each do |row|
      oid = table.headers.include?(pid_column) ? row[pid_column] : nil
      if oid && (km = radii[oid])
        row['properties'] = JSON.generate({ certainty_radius: km })
        matched += 1
      else
        row['properties'] = nil
      end
    end

    CSV.open(places_path, 'w') do |csv|
      csv << headers
      table.each { |row| csv << headers.map { |h| row[h] } }
    end

    puts "Enriched places.csv: #{matched}/#{table.length} places given a certainty_radius " \
         "(#{radii.size} overrides loaded). `properties` placed after `longitude`."
  end
end

if __FILE__ == $PROGRAM_NAME
  require 'dotenv'
  script_dir = File.dirname(File.expand_path(__FILE__))
  env = Dotenv.parse(File.join(script_dir, '.env.staging'))
  RelnetCertaintyRadius.enrich(
    places_path:    File.expand_path('data/relnet/output/places.csv'),
    overrides_path: File.expand_path('data/relnet/input/certainty_radius.csv'),
    env: env
  )
end
