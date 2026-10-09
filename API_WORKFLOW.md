# AutoFlow: API-ready workflow

## What changed

- Added `examples/live_geocoding_api.yaml`, a dry-run configuration for a public JSON API.
- Added `examples/live_geocoding_rules.yaml` with explicit checks for required names and valid latitude/longitude ranges.
- Added support for `source.url_env` in the REST connector, so private API endpoints can be configured without hardcoding the URL.
- Added a unit test for environment-based API URL resolution.

## Run on Windows

From the project root in the VS Code terminal:

```powershell
python -m pip install -e .
python -m autoflow connectors
python -m autoflow run --config examples/live_geocoding_api.yaml --dry-run
```

For machine-readable output:

```powershell
python -m autoflow run --config examples/live_geocoding_api.yaml --dry-run --json
```

The public example queries the Open-Meteo geocoding endpoint for Pune. It needs working internet access. The current sandbox could not resolve external hostnames, so the configuration and rules were validated locally, but a real API response could not be confirmed here.

## Important safety boundary

Start with `--dry-run`. Do not add `--commit` or enable recovery until you have inspected a successful live response and confirmed the rules. A network failure should appear as a failed run, not as an empty successful dataset.

## Authenticated APIs

Set an environment variable containing the URL and reference its name in config:

```powershell
$env:AUTOFLOW_API_URL = "https://your-api.example/v1/records"
$env:AUTOFLOW_API_TOKEN = "replace-with-your-token"
```

```yaml
source:
  type: rest_api
  url_env: AUTOFLOW_API_URL
  headers_env:
    Authorization: AUTOFLOW_API_TOKEN
  records_path: data.items
  timeout: 15
  max_pages: 10
```

Never commit real tokens or secrets to source control. Change `records_path` and pagination settings to match the API's documented JSON response. The current REST connector supports GET requests and expects a list of record objects, possibly nested under `records_path`.
