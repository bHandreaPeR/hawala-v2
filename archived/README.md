# archived/

Code that is **no longer in the live system or active research path**, kept
only for historical reference and to make sure pkill / search don't surface
it as "missing."

| Subfolder | What it was | Why archived |
|---|---|---|
| `v2_legacy/` | `alert_runner.py`, `alerts/vp_live_daemon.py`, `alerts/vp_signal_alert.py` | Replaced by v3 live runners. `alert_runner.py` still pkilled defensively in cron. |
| `notebook_cells/` | `cell_1_setup.py` … `cell_9_vwap_reversion.py` | Notebook export from April research. Strategy logic re-written into `strategies/*.py` since. |
| `notebooks_consolidated/` | `Hawala 2.ipynb`, `Hawala_v2_consolidated*.ipynb`, `Tester.ipynb` | Old consolidated notebooks. Useful as historical narrative; not loaded anywhere. |
| `scratch/` | `Untitled.ipynb`, `Untitled1.ipynb`, `scratchpad.html`, `-f`, `token.env.rtf` | One-off scratch + accidental files. |

**Don't reuse from here without checking the current strategies/ directory
first** — most of this code has a more recent equivalent there.
