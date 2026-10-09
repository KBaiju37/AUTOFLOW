"""Simple Tkinter demonstration GUI for AutoFlow. Uses the current Python environment."""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

ROOT = Path(__file__).resolve().parent

class AutoFlowGUI(tk.Tk):
    BG = "#0b1220"
    PANEL = "#121d30"
    PANEL2 = "#18263c"
    TEXT = "#edf3ff"
    MUTED = "#a7b6cc"
    ACCENT = "#6ee7b7"
    BLUE = "#7cb7ff"

    def __init__(self):
        super().__init__()
        self.title("AutoFlow | Data Reliability Console")
        self.geometry("1120x760")
        self.minsize(900, 640)
        self.configure(bg=self.BG)
        self.busy = False
        self.config_var = tk.StringVar(value="examples/live_geocoding_api.yaml")
        self.status_var = tk.StringVar(value="Ready • Dry-run mode")
        self._style()
        self._build()

    def _style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("TCombobox", fieldbackground=self.PANEL2, background=self.PANEL2,
                        foreground=self.TEXT, arrowcolor=self.TEXT, padding=6)
        style.configure("Treeview", background=self.PANEL, fieldbackground=self.PANEL,
                        foreground=self.TEXT, rowheight=28, borderwidth=0)

    def _label(self, parent, text, size=10, color=None, bold=False):
        return tk.Label(parent, text=text, bg=parent.cget("bg"), fg=color or self.TEXT,
                        font=("Segoe UI", size, "bold" if bold else "normal"))

    def _button(self, parent, text, command, primary=False):
        bg = "#177c67" if primary else self.PANEL2
        fg = "#ffffff" if primary else self.TEXT
        return tk.Button(parent, text=text, command=command, bg=bg, fg=fg,
                         activebackground="#269a80" if primary else "#263954",
                         activeforeground="#ffffff", relief="flat", bd=0,
                         padx=14, pady=10, cursor="hand2", font=("Segoe UI", 10, "bold"))

    def _build(self):
        header = tk.Frame(self, bg=self.BG, padx=26, pady=22)
        header.pack(fill="x")
        tk.Label(header, text="AutoFlow", bg=self.BG, fg=self.TEXT,
                 font=("Segoe UI", 25, "bold")).pack(anchor="w")
        tk.Label(header, text="DATA INGESTION  /  QUALITY GATES  /  RELIABILITY",
                 bg=self.BG, fg=self.ACCENT, font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(3, 0))
        tk.Label(header, text="A lightweight control panel for the AutoFlow project built so far.",
                 bg=self.BG, fg=self.MUTED, font=("Segoe UI", 10)).pack(anchor="w", pady=(8, 0))

        cards = tk.Frame(self, bg=self.BG, padx=22)
        cards.pack(fill="x")
        self._metric(cards, "SOURCE", "Live REST API", "Open-Meteo geocoding", 0)
        self._metric(cards, "QUALITY ENGINE", "Profile + validate", "Issues and rule checks", 1)
        self._metric(cards, "SAFETY", "Dry-run first", "No output writes from these actions", 2)

        content = tk.Frame(self, bg=self.BG, padx=22, pady=18)
        content.pack(fill="both", expand=True)
        left = tk.Frame(content, bg=self.PANEL, padx=18, pady=18, width=300)
        left.pack(side="left", fill="y", padx=(0, 14))
        left.pack_propagate(False)
        tk.Label(left, text="RUN WORKFLOWS", bg=self.PANEL, fg=self.TEXT,
                 font=("Segoe UI", 11, "bold")).pack(anchor="w")
        tk.Label(left, text="Choose a configuration", bg=self.PANEL, fg=self.MUTED,
                 font=("Segoe UI", 9)).pack(anchor="w", pady=(12, 5))
        self.combo = ttk.Combobox(left, textvariable=self.config_var, state="readonly",
                                  values=["examples/live_geocoding_api.yaml", "examples/autoflow.yaml"])
        self.combo.pack(fill="x", pady=(0, 8))
        self._button(left, "Browse config…", self._browse).pack(fill="x", pady=4)
        self._button(left, "▶  Run selected config (dry-run)", self._run_config, True).pack(fill="x", pady=(12, 5))
        ttk.Separator(left).pack(fill="x", pady=13)
        tk.Label(left, text="LOCAL DATA TOOLS", bg=self.PANEL, fg=self.TEXT,
                 font=("Segoe UI", 10, "bold")).pack(anchor="w", pady=(0, 5))
        self._button(left, "Profile sample CSV", self._profile).pack(fill="x", pady=4)
        self._button(left, "Validate sample CSV", self._validate).pack(fill="x", pady=4)
        self._button(left, "Show connector registry", self._connectors).pack(fill="x", pady=4)
        self._button(left, "Clear output", self._clear).pack(fill="x", pady=(14, 4))
        tk.Label(left, text="Safety: GUI actions use dry-run for configured workflows.\nNo commit or recovery action is exposed here.",
                 bg=self.PANEL, fg=self.MUTED, justify="left", wraplength=255,
                 font=("Segoe UI", 9)).pack(anchor="w", side="bottom", pady=(15, 0))

        right = tk.Frame(content, bg=self.PANEL, padx=16, pady=14)
        right.pack(side="left", fill="both", expand=True)
        bar = tk.Frame(right, bg=self.PANEL)
        bar.pack(fill="x")
        tk.Label(bar, text="EXECUTION LOG", bg=self.PANEL, fg=self.TEXT,
                 font=("Segoe UI", 11, "bold")).pack(side="left")
        tk.Label(bar, textvariable=self.status_var, bg=self.PANEL, fg=self.ACCENT,
                 font=("Segoe UI", 9)).pack(side="right")
        self.output = tk.Text(right, bg="#080e19", fg="#d9e5f7", insertbackground=self.TEXT,
                              relief="flat", wrap="word", padx=14, pady=12,
                              font=("Cascadia Mono", 9), state="disabled")
        self.output.pack(fill="both", expand=True, pady=(12, 0))
        self._write("Welcome to AutoFlow.\n\n"
                    "Choose ‘Run selected config (dry-run)’ to fetch live API data and assess it.\n"
                    "Or use the local CSV tools to demonstrate profiling and validation.\n\n"
                    "Results shown here are the actual CLI output; dry-run does not commit data.\n")

    def _metric(self, parent, title, value, note, col):
        frame = tk.Frame(parent, bg=self.PANEL, padx=17, pady=13)
        frame.grid(row=0, column=col, sticky="nsew", padx=(0 if col == 0 else 8, 0))
        parent.grid_columnconfigure(col, weight=1, uniform="metrics")
        tk.Label(frame, text=title, bg=self.PANEL, fg=self.MUTED,
                 font=("Segoe UI", 8, "bold")).pack(anchor="w")
        tk.Label(frame, text=value, bg=self.PANEL, fg=self.TEXT,
                 font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(5, 2))
        tk.Label(frame, text=note, bg=self.PANEL, fg=self.MUTED,
                 font=("Segoe UI", 8)).pack(anchor="w")

    def _write(self, text):
        self.output.configure(state="normal")
        self.output.insert("end", text.rstrip() + "\n")
        self.output.see("end")
        self.output.configure(state="disabled")

    def _clear(self):
        self.output.configure(state="normal")
        self.output.delete("1.0", "end")
        self.output.configure(state="disabled")
        self.status_var.set("Ready • Dry-run mode")

    def _browse(self):
        chosen = filedialog.askopenfilename(title="Choose AutoFlow YAML configuration",
                    initialdir=ROOT, filetypes=[("YAML files", "*.yaml *.yml"), ("All files", "*.*")])
        if chosen:
            try:
                self.config_var.set(Path(chosen).resolve().relative_to(ROOT).as_posix())
            except ValueError:
                messagebox.showerror("Config outside project", "Please choose a YAML file inside the AutoFlow project folder.")

    def _start(self, label, args):
        if self.busy:
            return
        self.busy = True
        self.status_var.set("Running…")
        self._write(f"\n{'=' * 72}\n{label}\n$ {' '.join([sys.executable, '-m', 'autoflow', *args])}\n")
        threading.Thread(target=self._worker, args=(label, args), daemon=True).start()

    def _worker(self, label, args):
        env = os.environ.copy()
        try:
            result = subprocess.run([sys.executable, "-m", "autoflow", *args], cwd=ROOT,
                                    capture_output=True, text=True, timeout=120, env=env)
            output = result.stdout
            if result.stderr:
                output += ("\n[stderr]\n" if output else "") + result.stderr
            if not output.strip():
                output = "Command finished with no output."
            self.after(0, self._finish, label, result.returncode, output)
        except subprocess.TimeoutExpired:
            self.after(0, self._finish, label, 124, "Command timed out after 120 seconds. Check network access or API response.")
        except Exception as exc:
            self.after(0, self._finish, label, 2, f"Could not run AutoFlow: {exc}")

    def _finish(self, label, code, output):
        self._write(output)
        self._write(f"\nExit code: {code}  (0 = success; 1 = gate/validation did not pass; 2 = config/usage error)\n")
        self.status_var.set("Finished • exit " + str(code))
        self.busy = False

    def _run_config(self):
        config = self.config_var.get().strip()
        if not config:
            messagebox.showwarning("Choose a config", "Select a YAML configuration first.")
            return
        if not (ROOT / config).is_file():
            messagebox.showerror("Config not found", f"Could not find this config inside the project:\n{config}")
            return
        self._start("Configured workflow (dry-run)", ["run", "--config", config, "--dry-run"])

    def _profile(self):
        path = "examples/orders.csv"
        if not (ROOT / path).is_file():
            messagebox.showerror("Sample missing", f"Expected file: {path}")
            return
        self._start("Profile sample CSV", ["profile", "--input", path])

    def _validate(self):
        path = "examples/orders.csv"
        rules = "examples/orders_rules.yaml"
        if not (ROOT / path).is_file() or not (ROOT / rules).is_file():
            messagebox.showerror("Sample files missing", "Expected examples/orders.csv and examples/orders_rules.yaml")
            return
        self._start("Validate sample CSV (no writes)", ["validate", "--input", path, "--rules", rules])

    def _connectors(self):
        self._start("Connector registry", ["connectors"])

if __name__ == "__main__":
    AutoFlowGUI().mainloop()
