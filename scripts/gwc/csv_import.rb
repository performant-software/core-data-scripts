require 'dotenv'

require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

def is_falsy(value)
  # handle all the different kinds of falsy/empty values in this spreadsheet
  value.nil? or value.downcase.strip.in? ['', 'n', '\n', 'n/a', 'null', 'no', 'na', 'unknown']
end

def val_or_nil(value)
  # get nil if falsy, otherwise value
  is_falsy(value) ? nil : value
end

def handle_bool(value)
  # get true/nil for booleans
  is_falsy(value) ? nil : true
end

def handle_array(array)
  # create a string array with double quotes around each entry for ingest
  array = array.map {|entry| entry ? "\"#{entry.gsub('"', '\'')}\"" : nil }.compact
  "[#{array.join(',')}]"
end

def date_to_iso(date)
  if date.nil?
    nil
  elsif date < 0
    # handle BC dates by removing negative sign and appending BC
    date = date.to_s[1..-1]
    date = date.rjust(4, '0')
    "#{date}-01-01T05:00:00.000Z BC"
  else
    date = date == 0 ? 1 : date
    date = date.to_s.rjust(4, '0')
    "#{date}-01-01T05:00:00.000Z"
  end
end

def handle_dates(dates)
  # fuzzy date range in the json format expected by core data. all dates
  # in this spreadsheet are simply integers representing years
  dates[0] = Integer dates[0] rescue nil
  dates[1] = Integer dates[1] rescue nil
  dates.map! {|date| date_to_iso(date)}
  if !dates[0].nil? and !dates[1].nil?
    date_range = {
      range: true,
      accuracy: 0,
      start_date: dates[0],
      end_date: dates[1],
      description: ''
    }
    date_range.to_json
  else
    nil
  end
end

def parse_gwc
  # expects witnesses.csv, manuscripts.csv, and gpc.csv in input/gwc
  input = File.expand_path('input/gwc')
  output = File.expand_path('output/gwc')
  env = Dotenv.parse './scripts/gwc/.env.staging'

  items = []
  events = []
  people_names = []
  language_names = []
  material_names = []
  oragnization_names = []
  place_names = []
  place_coords = {}

  # UDF multi select values to add
  uniq_content_types = []
  uniq_forms = []
  uniq_researchers = []
  uniq_religions = []
  uniq_regions = []
  uniq_scripts = []
  uniq_script_formats = []

  # split the three flat CSVs into per-model CSVs
  CSV.foreach("#{input}/manuscripts.csv", headers: true) do |row|
    # handle multi-value field columns with the same name (e.g. "1", "2", "3")
    languages = (6..10).map {|n| row[n]}.concat [val_or_nil(row['upper_text_language'])].compact
    materials = (33..37).map {|n| row[n]}
    material_names.concat materials.map {|entry| val_or_nil(entry) }.compact
    forms = (39..41).map {|n| row[n]}
    uniq_forms.concat forms.map {|entry| val_or_nil(entry) }.compact
    religions = (48..50).map {|n| val_or_nil(row[n]) }.compact
    uniq_religions.concat religions.map {|entry| val_or_nil(entry) }.compact
    bibliography_links = (97..100).map {|n| row[n] }
    scripts = (13..17).map {|n| row[n]}
    uniq_scripts.concat scripts.map{|entry| val_or_nil(entry) }.compact
    script_formats = (25..29).map {|n| row[n]}
    uniq_script_formats.concat script_formats.map{|entry| val_or_nil(entry) }.compact

    # handle array in single field with separators
    if is_falsy(row['entry_researcher'])
      researchers = []
    elsif ','.in? row['entry_researcher']
      researchers = row['entry_researcher'].split ', '
    else
      researchers = row['entry_researcher'].split '; '
    end
    uniq_researchers.concat researchers.map {|entry| val_or_nil(entry) }.compact

    # handle dates
    dates = row['terminus_post_quem'], row['terminus_ante_quem']

    # handle languages
    language_names.concat languages.compact

    # handle organizations and places
    if not is_falsy(row['current_location'])
      oragnization_names.concat row['current_location'].split('; ').compact
    end
    if not is_falsy(row['findspot'])
      place_names.push(row['findspot'])
    end

    # handle content fields format "content type (content desc)""
    content_desc, content_type = nil, nil
    if row.key? 'content' and !row['content'].nil?
      content_match = row['content'].match /^(?<type>[^\(]+) \((?<desc>.+)\)$/m
      if content_match
        content_type = content_match[:type]
        content_desc = content_match[:desc]
        uniq_content_types.push(content_type)
      end
    end

    # handle place of composition
    if not is_falsy(row['place_of_composition'])
      pname = row['place_of_composition']
      place_names.push(pname)
      lat = Float row["latitude"] rescue nil
      lon = Float row["longitude"] rescue nil
      if lat and lon
        place_coords[pname] = {latitude: lat, longitude: lon}
      end
    end

    # handle numeric
    width = Float row['dimension_width'] rescue nil
    height = Float row['dimension_height'] rescue nil
    folios = Integer row['number_of_folios'] rescue nil

    # handle decorations
    page_dye = handle_bool(row['page dye (Y/N)'])
    luxury_ink = handle_bool(row['luxury ink (Y/N)'])
    frontispiece = handle_bool(row['frontispiece (Y/N)'])
    full_page = handle_bool(row['full page illustrations (Y/N)'])
    half_page = handle_bool(row['half page illustrations (Y/N)'])
    interlinear = handle_bool(row['interlinear or marginal illustrations (Y/N)'])
    decor_binding = handle_bool(row['decorated binding/cover/case (Y/N)'])
    decor_initials = handle_bool(row['decorated initials (Y/N)'])
    contains_decorations = [
      page_dye,
      luxury_ink,
      frontispiece,
      full_page,
      half_page,
      interlinear,
      decor_binding,
      decor_initials
    ].any?

    # create final item row hash
    items.push({
      project_model_id: env['PROJECT_MODEL_ID_ITEMS'],
      uuid: SecureRandom.uuid,
      name: row['manuscript'],
    }.merge({
      "udf_#{env['UDF_ITEMS_TYPE_UUID']}": 'Manuscript',
      "udf_#{env['UDF_ITEMS_OTHER_EVIDENCE_SUBCATEGORY_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_SIGNIFICANCE_UUID']}": val_or_nil(row['significance']),
      "udf_#{env['UDF_ITEMS_CONTENT_TYPE_UUID']}": content_type,
      "udf_#{env['UDF_ITEMS_CONTENT_DESCRIPTION_UUID']}": content_desc,
      "udf_#{env['UDF_ITEMS_DATE_RANGE_UUID']}": handle_dates(dates),
      "udf_#{env['UDF_ITEMS_MULTILINGUAL_UUID']}": handle_bool(row['multilingual']),
      "udf_#{env['UDF_ITEMS_MULTI_MANUSCRIPT_CORPUS_UUID']}": handle_bool(row['is_multi_mss_corpus']),
      "udf_#{env['UDF_ITEMS_FORMS_UUID']}": handle_array(forms),
      "udf_#{env['UDF_ITEMS_RELIGIONS_UUID']}": handle_array(religions),
      "udf_#{env['UDF_ITEMS_COLOPHON_UUID']}": handle_bool(row['colophon']),
      "udf_#{env['UDF_ITEMS_COLOPHON_SCRIBE_UUID']}": val_or_nil(row['scribe']),
      "udf_#{env['UDF_ITEMS_COLOPHON_PATRON_UUID']}": val_or_nil(row['patron']),
      "udf_#{env['UDF_ITEMS_COLOPHON_TEXT_UUID']}": val_or_nil(row['text_of_colophon']),
      "udf_#{env['UDF_ITEMS_COLOPHON_PLACE_UUID']}": val_or_nil(row['place_of_colophon']),
      "udf_#{env['UDF_ITEMS_COLOPHON_DATE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DIMENSION_WIDTH_UUID']}": width,
      "udf_#{env['UDF_ITEMS_DIMENSION_HEIGHT_UUID']}": height,
      "udf_#{env['UDF_ITEMS_NUMBER_OF_FOLIOS_UUID']}": folios,
      "udf_#{env['UDF_ITEMS_PALIMPSEST_UUID']}": handle_bool(row['palimpsest']),
      "udf_#{env['UDF_ITEMS_PALIMPSEST_UPPER_TEXT_CONTENT_UUID']}": val_or_nil(row['upper_text_content']),
      "udf_#{env['UDF_ITEMS_BINDINGS_BINDING_UUID']}": handle_bool(row['binding (Y/N)']),
      "udf_#{env['UDF_ITEMS_BINDINGS_COVER_UUID']}": handle_bool(row['cover (Y/N)']),
      "udf_#{env['UDF_ITEMS_BINDINGS_CONTAINER_UUID']}": handle_bool(row['container (Y/N)']),
      "udf_#{env['UDF_ITEMS_BINDINGS_DESCRIPTION_UUID']}": row['bindings'],
      "udf_#{env['UDF_ITEMS_CONTAINS_DECORATIONS_UUID']}": contains_decorations,
      "udf_#{env['UDF_ITEMS_DECORATION_PAGE_DYE_UUID']}": page_dye,
      "udf_#{env['UDF_ITEMS_DECORATION_LUXURY_INK_UUID']}": luxury_ink,
      "udf_#{env['UDF_ITEMS_DECORATION_FRONTISPIECE_UUID']}": frontispiece,
      "udf_#{env['UDF_ITEMS_DECORATION_FULL_PAGE_ILLUSTRATIONS_UUID']}": full_page,
      "udf_#{env['UDF_ITEMS_DECORATION_HALF_PAGE_ILLUSTRATIONS_UUID']}": half_page,
      "udf_#{env['UDF_ITEMS_DECORATION_INTERLINEAR_OR_MARGINAL_ILLUSTRATIONS_UUID']}": interlinear,
      "udf_#{env['UDF_ITEMS_DECORATION_DECORATED_BINDING_COVER_CASE_UUID']}": decor_binding,
      "udf_#{env['UDF_ITEMS_DECORATION_DECORATED_INITIALS_UUID']}": decor_initials,
      "udf_#{env['UDF_ITEMS_DECORATION_DESCRIPTION_UUID']}": row['decoration'],
      "udf_#{env['UDF_ITEMS_FINDSPOT_DESCRIPTION_UUID']}": val_or_nil(row['findspot']),
      "udf_#{env['UDF_ITEMS_SCIENTIFIC_ANALYSIS_UUID']}": val_or_nil(row['scientific_analysis']),
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_UUID']}": row['bibliography'],
      "udf_#{env['UDF_ITEMS_EXTERNAL_LINK_UUID']}": row['any'],
      "udf_#{env['UDF_ITEMS_EXTERNAL_LINK_2_UUID']}": row['any2'],
      "udf_#{env['UDF_ITEMS_WIKIPEDIA_URL_UUID']}": row['wikipedia'],
      "udf_#{env['UDF_ITEMS_PLEIADES_URL_UUID']}": row['pleiades'],
      "udf_#{env['UDF_ITEMS_GETTY_URL_UUID']}": row['getty'],
      "udf_#{env['UDF_ITEMS_TRISMEGISTOS_URL_UUID']}": row['trismegistos'],
      "udf_#{env['UDF_ITEMS_DIGITAL_SURROGATE_LINK_UUID']}": val_or_nil(row['digital_surrogate_link']),
      "udf_#{env['UDF_ITEMS_DIGITAL_SURROGATE_LABEL_UUID']}": val_or_nil(row['digital_surrogate_label']),
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_1_UUID']}": bibliography_links[0],
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_2_UUID']}": bibliography_links[1],
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_3_UUID']}": bibliography_links[2],
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_4_UUID']}": bibliography_links[3],
      "udf_#{env['UDF_ITEMS_WITNESS_TEXT_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_ENTRY_RESEARCHER_UUID']}": handle_array(researchers),
    }.sort_by { |key| key }.to_h))
  end

  CSV.foreach("#{input}/witnesses.csv", headers: true) do |row|
    # handle authors
    authors = row['Author'].split("; ")
    people_names.concat authors
    # handle dates
    dates = row['Earliest'], row['Latest']
    # handle materials
    materials = (row['Substrate(s)'] || '').split(',')
    material_names.concat materials.map {|entry| val_or_nil(entry) }.compact
    # create final item row
    items.push({
      project_model_id: env['PROJECT_MODEL_ID_ITEMS'],
      uuid: SecureRandom.uuid,
      name: row['Work'],
    }.merge({
      "udf_#{env['UDF_ITEMS_TYPE_UUID']}": 'Other Evidence',
      "udf_#{env['UDF_ITEMS_OTHER_EVIDENCE_SUBCATEGORY_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_SIGNIFICANCE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_CONTENT_TYPE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_CONTENT_DESCRIPTION_UUID']}": row['Content'],
      "udf_#{env['UDF_ITEMS_DATE_RANGE_UUID']}": handle_dates(dates),
      "udf_#{env['UDF_ITEMS_MULTILINGUAL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_MULTI_MANUSCRIPT_CORPUS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_FORMS_UUID']}": row['Form(s)'] ? handle_array(row['Form(s)'].split(',')) : [],
      "udf_#{env['UDF_ITEMS_RELIGIONS_UUID']}": [],
      "udf_#{env['UDF_ITEMS_COLOPHON_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_COLOPHON_SCRIBE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_COLOPHON_PATRON_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_COLOPHON_TEXT_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_COLOPHON_PLACE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_COLOPHON_DATE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DIMENSION_WIDTH_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DIMENSION_HEIGHT_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_NUMBER_OF_FOLIOS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_PALIMPSEST_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_PALIMPSEST_UPPER_TEXT_CONTENT_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BINDINGS_BINDING_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BINDINGS_COVER_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BINDINGS_CONTAINER_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BINDINGS_DESCRIPTION_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_CONTAINS_DECORATIONS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_PAGE_DYE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_LUXURY_INK_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_FRONTISPIECE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_FULL_PAGE_ILLUSTRATIONS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_HALF_PAGE_ILLUSTRATIONS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_INTERLINEAR_OR_MARGINAL_ILLUSTRATIONS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_DECORATED_BINDING_COVER_CASE_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_DECORATED_INITIALS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DECORATION_DESCRIPTION_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_FINDSPOT_DESCRIPTION_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_SCIENTIFIC_ANALYSIS_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_EXTERNAL_LINK_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_EXTERNAL_LINK_2_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_WIKIPEDIA_URL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_PLEIADES_URL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_GETTY_URL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_TRISMEGISTOS_URL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DIGITAL_SURROGATE_LINK_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_DIGITAL_SURROGATE_LABEL_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_1_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_2_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_3_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_BIBLIOGRAPHY_LINK_4_UUID']}": nil,
      "udf_#{env['UDF_ITEMS_WITNESS_TEXT_UUID']}": row['Text/Image'],
      "udf_#{env['UDF_ITEMS_ENTRY_RESEARCHER_UUID']}": [],
    }.sort_by { |key| key }.to_h))
  end

  # geopolitical contexts
  CSV.foreach("#{input}/gpc.csv", headers: true) do |row|
    # handle 2-column bibliography
    bibliography = row['bibliography']
    if (row['bibliography_2'] || '').strip != ''
      bibliography << "; #{row['bibliography_2']}"
    end

    # handle region
    uniq_regions.push(row['region'])

    # handle dates
    dates = row['start_date'], row['end_date']
    dates[0] = Integer dates[0] rescue nil
    dates[1] = Integer dates[1] rescue nil
    dates.map! {|date| date_to_iso(date)}

    # create final item row
    events.push({
      project_model_id: env['PROJECT_MODEL_ID_GEOPOLITICAL_CONTEXTS'],
      uuid: SecureRandom.uuid,
      name: row['name'],
      description: nil,
    }.merge({
      start_date: dates[0],
      start_date_description: nil,
      end_date: dates[1],
      end_date_description: nil,
      "udf_#{env['UDF_EVENTS_REGION_UUID']}": row['region'],
      "udf_#{env['UDF_EVENTS_PERIODO_LABEL_UUID']}": row['periodo_label'],
      "udf_#{env['UDF_EVENTS_PERIODO_REGION_UUID']}": row['periodo_region'],
      "udf_#{env['UDF_EVENTS_PERIODO_URL_UUID']}": row['periodo'],
      "udf_#{env['UDF_EVENTS_WIKIDATA_URL_UUID']}": row['wikidata'],
      "udf_#{env['UDF_EVENTS_PERIODO_AUTHORITY_URL_UUID']}": row['periodo_authority'],
      "udf_#{env['UDF_EVENTS_BIBLIOGRAPHY_UUID']}": bibliography,
      "udf_#{env['UDF_EVENTS_WIKIPEDIA_URL_UUID']}": row['wikipedia'],
    }.sort_by { |key| key }.to_h))
  end

  # build the other model CSV hash arrays
  people = []
  Set.new(people_names).each do |person_name|
    people.push({
      project_model_id: env['PROJECT_MODEL_ID_PEOPLE'],
      uuid: SecureRandom.uuid,
      last_name: person_name,
      first_name: nil,
      middle_name: nil,
      biography: nil,
    })
  end
  organizations = []
  Set.new(oragnization_names).each do |oragnization_name|
    organizations.push({
      project_model_id: env['PROJECT_MODEL_ID_ORGANIZATIONS'],
      uuid: SecureRandom.uuid,
      name: oragnization_name,
      description: nil,
    })
  end
  taxonomies = []
  Set.new(language_names).each do |lang_name|
    taxonomies.push({
      project_model_id: env['PROJECT_MODEL_ID_LANGUAGES'],
      uuid: SecureRandom.uuid,
      name: lang_name,
    })
  end
  Set.new(material_names).each do |material_name|
    taxonomies.push({
      project_model_id: env['PROJECT_MODEL_ID_MATERIALS'],
      uuid: SecureRandom.uuid,
      name: material_name,
    })
  end
  places = []
  Set.new(place_names).each do |place_name|
    lat = place_coords.key?(place_name) ? place_coords[place_name][:latitude] : nil
    lon = place_coords.key?(place_name) ? place_coords[place_name][:longitude] : nil
    places.push({
      project_model_id: env['PROJECT_MODEL_ID_PLACES'],
      uuid: SecureRandom.uuid,
      name: place_name,
      latitude: lat,
      longitude: lon,
    })
  end

  model_files = {
    'events': events,
    'items': items,
    'organizations': organizations,
    'people': people,
    'places': places,
    'taxonomies': taxonomies
  }

  filepaths = []

  # simplified csv generation
  model_files.keys.each do |key|
    path = "#{output}/#{key}.csv"
    filepaths.push(path)
    model_rows = model_files[key]
    CSV.open(path, 'w', headers: model_rows.first.keys, write_headers: true) do |csv|
      model_rows.each do |row|
        csv << row.values
      end
    end
  end

  # uncomment to get unique values for multi-select fields in order to update project config
  # puts handle_array(Set.new(uniq_content_types).to_a)
  # puts handle_array(Set.new(uniq_forms).to_a)
  # puts handle_array(Set.new(uniq_regions).to_a)
  # puts handle_array(Set.new(uniq_religions).to_a)
  # puts handle_array(Set.new(uniq_researchers).to_a)
  # puts handle_array(Set.new(uniq_scripts).to_a)
  # puts handle_array(Set.new(uniq_script_formats).to_a)

  archive = Archive.new
  archive.create_archive(filepaths, output)
end
