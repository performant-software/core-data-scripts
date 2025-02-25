# just a litlte helper script to quickly get UDF UUIDs into the correct format for environment vars

def print_uuids(project_model_id, project_model_name)
  # project_model_id should be an integer, project_model_name should be plural name string e.g. "items"
  udfs = CoreDataConnector::ProjectModel.find(project_model_id).user_defined_fields
  udfs.each do |udf|
    uname = udf.column_name.parameterize.upcase.gsub '-', '_'
    uuid = udf.uuid.gsub '-', '_'
    puts "UDF_#{project_model_name.upcase}_#{uname}_UUID=\"#{uuid}\""
  end
end
