require 'active_support/core_ext/string/filters'
require 'csv'
require 'dotenv'
require 'optparse'
require 'securerandom'

class CsvTransform
  def initialize(input:, output:)
    @input_path = input
    @output_path = output

    @filenames = [
      'archives',
      'editions_archives',
      'editions_editions',
      'editions_people',
      'editions_publishers',
      'editions',
      'people',
      'places',
      'publishers_places',
      'publishers',
      'works'
    ]
  end

  def copy_to_output_dir
    @filenames.each do |filename|
      FileUtils.cp("#{@input_path}/#{filename}.csv", @output_path)
    end
  end

  def add_uuids
    @filenames.each do |filename, idx|
      CSV.open("#{@output_path}/#{filename}.csv", 'wb', write_headers: true) do |csv_out|
        table = CSV.read("#{@input_path}/#{filename}.csv", headers: true)

        table.each do |row|
          row['uuid'] = SecureRandom.uuid
        end

        csv_out << table[0].to_h.keys
        csv_out << table
      end

    end
  end

  env = Dotenv.parse './scripts/rumpf/.env.development'
end

# Parse input options
options = {}

OptionParser.new do |opts|
  opts.on '-i INPUT', '--input INPUT', 'Input directory'
  opts.on '-o OUTPUT', '--output OUTPUT', 'Output directory'
end.parse!(into: options)

unless options[:input] && options[:output]
  puts 'Input and output directory paths are required in arguments.'
  exit 1
end

transform = CsvTransform.new(
  input: options[:input],
  output: options[:output]
)

transform.add_uuids
