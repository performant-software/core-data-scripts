# Core Data Transformation Scripts

This repo contains the scripts for several projects to transform their data from CSVs into the format required by Core Data's import service.

## How to run

The top-level program knows which paths to use based on the project name, so that is the only required CLI argument.

Begin by copying your CSVs into `/input/{project name}`, e.g. `/input/gca`. Then run:

```bash
./transform.rb -p {project name}
```

The project name should match the name of one of the directories under `scripts`.
