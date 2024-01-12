require 'zip'

class Archive
  def create_archive(filepaths, output_path, remove_files = false)
    # Create the zipfile name
    zipfile_name = "#{output_path}/archive.zip"

    # Clear out the previous zipfile if one exists
    if File.file?(zipfile_name)
      File.delete(zipfile_name)
    end

    # Add each of the passed files to the archive
    Zip::File.open(zipfile_name, create: true) do |zipfile|
      filepaths.each do |filepath|
        filename = File.basename(filepath)
        zipfile.add(filename, filepath)
      end
    end

    # Delete the individual files
    filepaths.each { |f| File.delete(f) } if remove_files
  end
end
