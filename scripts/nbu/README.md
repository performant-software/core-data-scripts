# NBU Script Readme

## User-defined fields

Since user-defined fields are stored in a `user_defined` `jsonb` column, we need to use a special query that looks like:

```sql
select user_defined->'Category' as category from documents;
```

Instead of handling the `user_defined` object (which would add complexity), the script will assume that you've extracted each user-defined field into its own CSV column using similar queries to the above example. User-defined field names should be converted to snake case, e.g. `Identity Labels` -> `identity_labels`.

We should skip unused UDFs to save time. It might also be worth skipping ones with a very small number of values.

### Documents

* Category (2 values)
* Contains (3 values)

### Documents-People

* XML ID

### Documents-Places

* XML ID

### People

* Identity Labels
* Approximate Birth Year
* Alternate Name 1 (just 5 values and will require special handling - maybe skip?)

### Places

* Category (1 value)

### Summary

The UDFs to definitely import are Identity Labels and Approximate Birth Year from the People table, and of course the XML ID fields in the document relations. The rest might be skippable.

Alternate Name 1 should be skipped if at all possible - we'd need to add handling to ingest it into the `names` array in Core Data instead of a user-defined field. If we really must ingest this field, I would probably just make a note of those five records and manually add the alternate names after import.

The other low-value-count ones won't take a ton of time but I'd still rather avoid adding complexity to the script when not needed.

## Relations

* events_people
* events_places
* documents_events
* documents_people
* documents_places
* people_people (family relations)
* people_people (enslavement relations)
* people_places
* people_identity_labels
