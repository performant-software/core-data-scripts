require 'active_support/core_ext/string/filters'
require 'dotenv'
require 'optparse'

require_relative '../../core/csv/adapter'
require_relative '../../core/archive'

class CsvImport < Csv::Adapter
  def cleanup
    super

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_archives
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_editions
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_people
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_places
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_publishers
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_works
    SQL

    execute <<-SQL.squish
      DROP TABLE IF EXISTS z_relationships
    SQL
  end

  # Parse environment variables
  env = Dotenv.parse './scripts/gbof/.env.development'

  # Parse input options
  options = {}

  OptionParser.new do |opts|
    opts.on '-d DATABASE', '--database DATABASE', 'Database name'
    opts.on '-u USER', '--user USER', 'Database username'
    opts.on '-f FILE', '--file FILE', 'Source filepath'
    opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
  end.parse!(into: options)

  # Run the importer
  import = CsvImport.new(
    database: options[:database],
    user: options[:user],
    filepath: options[:file],
    output: options[:output],
    env: env
  )

  import.cleanup

  filepaths = [
    "#{options[:output]}/organizations.csv",
    "#{options[:output]}/people.csv",
    "#{options[:output]}/places.csv",
    "#{options[:output]}/relationships.csv"
  ]

  archive = Archive.new
  archive.create_archive(filepaths, options[:output])
end