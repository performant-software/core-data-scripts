# frozen_string_literal: true

require 'json'

# Shared geometry helpers for FairData migrations.
#
# nodegoat (and GeoJSON generally) order coordinates [longitude, latitude];
# FairData stores latitude/longitude — so every accessor here returns
# [latitude, longitude]. Also provides circle-polygon radius math for
# positional uncertainty (certainty_radius), derived from turf-style circle
# polygons whose vertices are ~equidistant from the center.
#
# Ruby 2.6 compatible.
module Geo
  EARTH_RADIUS_KM = 6371.0088

  module_function

  # Great-circle distance in km between two [lon, lat] points.
  def haversine_km(lon1, lat1, lon2, lat2)
    rad = ->(deg) { deg * Math::PI / 180.0 }
    dlat = rad.call(lat2 - lat1)
    dlon = rad.call(lon2 - lon1)
    a = (Math.sin(dlat / 2)**2) +
        (Math.cos(rad.call(lat1)) * Math.cos(rad.call(lat2)) * (Math.sin(dlon / 2)**2))
    2 * EARTH_RADIUS_KM * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  end

  # Centroid of a ring [[lon, lat], ...] -> [latitude, longitude] (rounded to 6dp).
  # Drops the closing duplicate vertex if present.
  def centroid(ring)
    return [nil, nil] if ring.nil? || ring.empty?
    verts = ring.last == ring.first ? ring[0..-2] : ring
    return [nil, nil] if verts.empty?
    lon = verts.map { |p| p[0].to_f }.sum / verts.length
    lat = verts.map { |p| p[1].to_f }.sum / verts.length
    [lat.round(6), lon.round(6)]
  end

  # Mean radius (km) of a circle polygon ring [[lon, lat], ...], measured from
  # its centroid. turf.circle() vertices are ~equidistant; averaging smooths
  # float drift.
  def polygon_radius_km(ring)
    return 0.0 if ring.nil? || ring.empty?
    verts = ring.last == ring.first ? ring[0..-2] : ring
    return 0.0 if verts.empty?
    clon = verts.map { |p| p[0].to_f }.sum / verts.length
    clat = verts.map { |p| p[1].to_f }.sum / verts.length
    distances = verts.map { |p| haversine_km(clon, clat, p[0].to_f, p[1].to_f) }
    distances.sum / distances.length
  end

  # Parse a GeoJSON geometry string -> [latitude, longitude].
  # Point -> its coordinates; Polygon / GeometryCollection -> centroid of the
  # (collected) vertices. Returns [nil, nil] on empty/unparseable input.
  def parse_point(geojson)
    return [nil, nil] if geojson.nil? || geojson.to_s.strip.empty?
    geo = JSON.parse(geojson)
    case geo['type']
    when 'Point'
      lon, lat = geo['coordinates']
      [lat, lon]
    when 'Polygon'
      centroid(geo.dig('coordinates', 0))
    when 'GeometryCollection'
      pts = (geo['geometries'] || []).flat_map { |g| member_coords(g) }
      centroid(pts)
    else
      [nil, nil]
    end
  rescue JSON::ParserError
    [nil, nil]
  end

  def member_coords(geom)
    case geom['type']
    when 'Point'   then [geom['coordinates']]
    when 'Polygon' then geom.dig('coordinates', 0) || []
    else []
    end
  end
end
