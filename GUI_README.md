# AutoFlow demonstration GUI

A basic Windows-friendly desktop GUI built with Python's standard-library Tkinter. It wraps the existing AutoFlow CLI; it does not replace the engine or add new pipeline capabilities.

## Start on Windows

1. Open this project folder in VS Code.
2. Activate the project's virtual environment in the VS Code terminal (if used):
   ` .\.venv\Scripts\Activate.ps1 `
3. Start the GUI with:
   `python gui.py`

Or double-click `run_gui.bat`. It uses `.venv` or `trail` if present, otherwise it uses the `python` on PATH.

## Available actions

- Run selected YAML configuration in **dry-run mode** (default API example is `examples/live_geocoding_api.yaml`).
- Profile the bundled `examples/orders.csv` file.
- Validate the bundled CSV against `examples/orders_rules.yaml`.
- Display the connector registry.

The GUI intentionally does not expose `--commit` or recovery application buttons. Review CLI output and configuration before writing data. The live API action requires internet access. Tkinter is included with most standard Windows Python installations; if Python was installed without Tcl/Tk support, install a standard Python distribution that includes Tcl/Tk.
