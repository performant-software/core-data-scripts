# RelNet migration (nodegoat → FairData)

Transforms the RelNet nodegoat exports into FairData (Core Data Cloud) import CSVs.
RelNet: religious networks and sacred itineraries in late-1st-millennium-BCE
Babylonia (University of Barcelona, PI Rocío Da Riva). See the data-model spec
and decisions in `~/repos/relnet/fairdata-model.md` and
`~/repos/relnet/docs/data-mapping.md`.

## Layout

```
scripts/relnet/
  setup_project.rb              # create the FairData project (models/UDFs/relationships) via API → writes .env.staging
  csv_import.rb                 # transform nodegoat exports → FairData CSVs + relationships
  enrich_certainty_radius.rb    # add place positional uncertainty (meters → km) to places.csv
  .env.staging                  # IDs/UUIDs (placeholders until setup_project.rb runs)
  id_maps/                      # persistent source-ID → UUID maps (commit these for stable re-imports)
data/relnet/
  input/                        # copy the 4 nodegoat exports here (see below)
  intermediate/                 # generated singular-row CSVs
  output/                       # generated FairData import CSVs
```

## Prerequisites

- A working Ruby with `dotenv` + `activesupport` gems (the repo Gemfile). NOTE: the
  rvm default `ruby-2.6.3` is currently broken on this machine (x86_64/arm64 gmp
  mismatch). Use a working arm64 Ruby (`/usr/bin/ruby` 2.6.10 works for syntax;
  for a full run use a Ruby where `bundle install` succeeds).
- FairData **staging admin credentials** (email/password) for `setup_project.rb`.
  Not yet located in 1Password — retrieve or create a service account first.
- Copy the exports:
  `cp ~/repos/relnet/nodegoat-export/*.csv data/relnet/input/`

## Run order

```bash
# 1. Create the FairData project and capture real IDs/UUIDs into .env.staging
ruby scripts/relnet/setup_project.rb <email> <password> staging

# 2. Verify UDF UUIDs map to the right column_name (guards the order-mapping pitfall)
#    GET /core_data/project_models/{id} and compare column_name to each .env key

# 3. Transform exports → data/relnet/output/{places,items,organizations,people,taxonomies,relationships}.csv
ruby scripts/relnet/csv_import.rb staging

# 4. Import via the FairData admin UI (staging first), models before relationships,
#    or in one batch (records + relationships must share the batch).
```

## What it produces (Phase A+)

Records: Places (254), Tablets/Items (144), Museums/Organizations (15),
Cultic Actors (136) + Divine Characters (440) → `people.csv`, and the controlled
vocabularies (Place Types, Writing Classifications, Genders, Divine Capacities)
→ `taxonomies.csv`.

Relationships: Tablet→Museum (Held at), Tablet→Place (Findspot, ~132),
Place→Place (Part of, 11), Place→Place Type, Tablet→Writing, Person→Gender,
Divine→Capacity, Divine→Divine (Alternative name for, 24).

## Modeling notes / open items

- **Categorical fields are modeled as Taxonomy + relationship** (Place Type,
  Writing, Gender, Capacity), per the pstudio-data-model recommendation — their
  relationship names are auto-faceted in Typesense. Flip to Select UDFs in
  `setup_project.rb` if the client prefers.
- **`date_gregorian` is left empty** — deriving Gregorian years from cuneiform
  regnal dates ("Nbk 22/I/23") needs a reign→year table. The FuzzyDate UDF exists
  so the timeline `year_facet` works once populated.
- **`certainty_radius`** is in km (meters/1000). Supply per-place meter radii via
  an optional `data/relnet/input/certainty_radius.csv` (`nodegoat_object_id,radius_m`),
  e.g. the Ziggurat of Babylon and sub-temple features.
- **Phase B** "mentioned" cross-references (Tablet→Place/Deity/Actor) and References
  await the nodegoat tablet multi-value exports. Their relationship definitions
  already exist in `setup_project.rb`.

## Verification done (against real exports, 2026-06-01)

GeoJSON `[lon,lat]→lat,lon` swap correct (Enamhe Temple → 32.53,44.41; Ziggurat
polygon → centroid); Provenance→Findspot matches 132/134 (City-stem matching);
Part-of resolves 11/11 by name; Museums extract to 15; Divine self-refs 24/24.
