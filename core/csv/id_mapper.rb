require 'json'

module Csv
  class IdMapper
    def initialize(csv_path:, json_path:, id_column:)
      @csv_path = csv_path
      @json_path = json_path
      @id_column = id_column
    end

    def get_hashmap
      begin
        file = File.read(@json_path)
        JSON.parse(file)
      rescue
        return {}
      end
    end

    def write_hashmap(obj)
      File.write(@json_path, obj.to_json)
    end
  end
end
