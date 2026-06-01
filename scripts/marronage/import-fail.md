The import service silently drops any CSV columns not declared in its column_names whitelist, and properties isn't in that list.

places.rb (import service, lines 118–149) only accepts: project_model_id, uuid, name, latitude, longitude, place_id, user_defined, import_id. That's it.

Two things need to happen in core-data-connector:

1. Migration: add a properties jsonb column to the core_data_connector_places table
2. Import service: add { name: 'properties', copy: true } to the column_names method in app/services/core_data_connector/import/places.rb

Without #2, the PostgreSQL COPY silently skips the column — no error, no data saved.

You mentioned you can see the properties attribute in the API response — that might be Camden adding it to the serializer but not yet wiring up the import path. Worth checking with him whether the migration (#1) has landed on staging and whether he's aware the import service (#2) also needs updating.