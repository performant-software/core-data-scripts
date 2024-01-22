#!/usr/bin/env ruby

require 'optparse'

require_relative './scripts/bischoff/csv_import.rb'
require_relative './scripts/gca/csv_import.rb'
require_relative './scripts/rumpf/csv_import.rb'
require_relative './scripts/supplique/csv_import.rb'

def create_output_dir(project)
  dir_name = File.expand_path("output/#{project}")
  Dir.mkdir(dir_name) unless File.exist?(dir_name)
end

def main
  # Parse input options
  options = {}

  OptionParser.new do |opts|
    opts.on '-p PROJECT', '--project PROJECT', 'Project name'
  end.parse!(into: options)

  unless options[:project]
    puts 'Project name is required in arguments.'
    exit 1
  end

  create_output_dir(options[:project])

  case options[:project]
  when 'supplique'
    parse_supplique
  when 'gca'
    parse_gca
  when 'rumpf'
    parse_rumpf
  when 'bischoff'
    parse_bischoff
  else
    puts 'No matching project found.'
    exit 1
  end
end

main if __FILE__ == $PROGRAM_NAME
