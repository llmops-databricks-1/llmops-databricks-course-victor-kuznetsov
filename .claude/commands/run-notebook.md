Deploy and run a notebook on Databricks using Databricks Asset Bundles.

The argument is the notebook path relative to the repo root (e.g. `notebooks/hello_world.py`).

## Steps

### 1. Determine the job resource name

Derive a job resource key from the notebook filename: take the filename without extension, replace hyphens with underscores, append `_job`.

Example: `notebooks/hello_world.py` → resource key `hello_world_job`.

### 2. Validate `resources/` folder exists and is included in `databricks.yml`

Check that a `resources/` directory exists at the repo root. If not, create it.

Check that `databricks.yml` includes the resources folder. It must contain an `include` block like:

```yaml
include:
  - resources/*.yml
```

If the `include` block is missing, add it to `databricks.yml` directly under the `bundle:` section.

### 3. Check for an existing job resource file

Look for `resources/<resource_key>.yml`. If it already exists, skip creation and go to step 5.

### 4. Create the job resource file

Create `resources/<resource_key>.yml` with this exact structure (substitute `<resource_key>`, `<job_display_name>`, and `<notebook_path>` with the actual values):

```yaml
resources:
  jobs:
    <resource_key>:
      name: <job_display_name>
      tags:
        project_name: "brickkit"

      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl

      tasks:
        - task_key: run_notebook
          environment_key: default
          notebook_task:
            notebook_path: <notebook_path>
            base_parameters:
              env: ${bundle.target}
              git_sha: "${var.git_sha}"
              run_id: "{{job.run_id}}"
```

Where:
- `<resource_key>` = the derived key (e.g. `basic_catalog_job`)
- `<job_display_name>` = kebab-case version of the notebook name (e.g. `basic-catalog`)
- `<notebook_path>` = the notebook path as given by the user (e.g. `examples/01_quickstart/basic_catalog.py`)

Also add the `git_sha` variable definition to `databricks.yml` if it is not already present:

```yaml
variables:
  git_sha:
    description: "Git SHA of the deployed commit"
    default: "local"
```

### 5. Build the wheel and deploy the bundle

Run:
```bash
databricks bundle deploy
```

### 6. Run the job

Run:
```bash
databricks bundle run <resource_key>
```

Where `<resource_key>` is the derived key from step 1.

Report the output to the user, including any run URL printed by the CLI.

## Example

```bash
/run-notebook notebooks/hello_world.py
```

This deploys and runs `notebooks/hello_world.py` as a Databricks job with resource key `hello_world_job`.
