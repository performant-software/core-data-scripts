# Just a quick helper script I wrote to go part of the way in
# generating the array of fields for the GCA import script.

require 'pp'

fields = 'Item Id,Item URI,Dublin Core:Title,Dublin Core:Subject,Dublin Core:Description,Dublin Core:Creator,Dublin Core:Source,Dublin Core:Publisher,Dublin Core:Date,Dublin Core:Contributor,Dublin Core:Rights,Dublin Core:Relation,Dublin Core:Format,Dublin Core:Language,Dublin Core:Type,Dublin Core:Identifier,Dublin Core:Coverage,Item Type Metadata:Race,Item Type Metadata:Building Stories,Item Type Metadata:Building Height,Item Type Metadata:Calculated Height,Item Type Metadata:Removed,Item Type Metadata:Occupants - Residents,Item Type Metadata:Occupants - Business/ Organization,Item Type Metadata:Description,Item Type Metadata:Building Type,Item Type Metadata:Building Use,Item Type Metadata:Date Removed,Item Type Metadata:Text,Item Type Metadata:geolocation:address,Item Type Metadata:geolocation:map_type,Item Type Metadata:geolocation:zoom_level,Item Type Metadata:geolocation:longitude,Item Type Metadata:geolocation:latitude,Item Type Metadata:County,Item Type Metadata:Elev_f,Item Type Metadata:Elev_m,Item Type Metadata:TopoName,Item Type Metadata:Title Alias,Item Type Metadata:Architect,Item Type Metadata:Resources,Item Type Metadata:Additional Use,Item Type Metadata:Coordinates,Item Type Metadata:Occupants - Entities,Item Type Metadata:Roof Height,Item Type Metadata:Base Height,Item Type Metadata:Date Built,Item Type Metadata:Identifier,Item Type Metadata:Rights,Item Type Metadata:Date,Item Type Metadata:Source,Item Type Metadata:Relation,Item Type Metadata:Compression,Item Type Metadata:To,Item Type Metadata:From,Item Type Metadata:Subject Line,Item Type Metadata:Email Body,Item Type Metadata:Time Summary,Item Type Metadata:Bit Rate/Frequency,Item Type Metadata:Director,Item Type Metadata:Producer,Item Type Metadata:CC,Item Type Metadata:Duration,Item Type Metadata:Physical Dimensions,Item Type Metadata:Original Format,Item Type Metadata:Local URL,Item Type Metadata:Transcription,Item Type Metadata:Location,Item Type Metadata:Interviewee,Item Type Metadata:Interviewer,Item Type Metadata:Event Type,Item Type Metadata:Name,Item Type Metadata:Bibliography,Item Type Metadata:Biographical Text,Item Type Metadata:Occupation,Item Type Metadata:Death Date,Item Type Metadata:Birthplace,Item Type Metadata:Birth Date,Item Type Metadata:Participants,Item Type Metadata:Street Adress,Item Type Metadata:URL,Item Type Metadata:Lesson Plan Text,Item Type Metadata:Materials,Item Type Metadata:Objectives,Item Type Metadata:Standards,Item Type Metadata:Number of Attachments,Item Type Metadata:BCC,tags,file,itemType,collection,public,featured'

fields_arr = fields.split(',')

config = fields_arr.map do |f|
  {
    name: f,
    # We can't automatically type these :(
    type: 'TODO'
  }
end

puts config.pretty_inspect
