require_relative '../../core/archive'
require_relative '../../core/csv/plain_csv_ingester'

class CsvTransform < Csv::PlainCsvIngester

end

model_files = [
  'people',
  'places',
  'taxonomies',
  'works'
]

fields = {
  people: {
    'last_name': 'last_name',
    'first_name': 'first_name',
    'middle_name': nil,
    'biography': nil,
    "udf_#{env['UDF_PEOPLE_TITLE_UUID']}": 'title',
    "udf_#{env['UDF_PEOPLE_TYPE_UUID']}": 'type',
    "udf_#{env['UDF_PEOPLE_BIRTH_DATE_UUID']}": 'birth_date',
    "udf_#{env['UDF_PEOPLE_DEATH_DATE_UUID']}": 'death_date'
  },
  places: {
    'name': 'name',
    'latitude': nil,
    'longitude': nil,
    "udf_#{env['UDF_PLACES_POSTAL_CODE_UUID']}": 'postal_code',
  },
  taxonomies: {
    'word': 'word'
  },
  works: {
    'name': 'name',
    "udf_#{env['UDF_WORKS_YEAR_UUID']}": 'year',
    "udf_#{env['UDF_WORKS_MONTH_UUID']}": 'month',
    "udf_#{env['UDF_WORKS_DAY_UUID']}": 'day',
    "udf_#{env['UDF_WORKS_SUMMARY_UUID']}": 'summary',
    "udf_#{env['UDF_WORKS_REFERENCES_UUID']}": 'references',
    "udf_#{env['UDF_WORKS_COMMENTS_UUID']}": 'comments',
    "udf_#{env['UDF_WORKS_TEXT_UUID']}": 'text',
  }
}

# TODO: relationships
