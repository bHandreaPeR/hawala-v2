# data_dumps/

Bulk binary / large-file outputs that are NOT imported by any code path.

| Subfolder | Contents | Producer |
|---|---|---|
| `newsletters/` | `Newsletter <DDth Month YY>.pdf` (spaces, no underscores) — current daily newsletter | `run_daily_report.py` (07:32 IST cron) |
| `newsletters_archive/` | Pre-rename `market_report_<date>.{pdf,html}` (Apr–May 2026) | older `run_daily_report.py` |
| `signals/` | `market_signal_<date>.json` raw payloads | `run_daily_report.py` |
| `nse_bhavcopy/` | `nsccl.*.spn / *.zip` margin files | downloaded manually |
| `fii_history/` | `fii_stats_*.xls` historic FII xls dumps | manual |
| `reference/` | API PDFs, ORB Excel, screenshots | manual |
| `trade_logs_archive/` | Old `trade_log_*.csv` from cells/notebooks | `cell_*` scripts (archived) |
| `combine_oi_archive/` | `combineoi_04052026/*` + zip | manual zip drop |
| `mar2025/` | `mar-2025.xlsx`, `Mar_2025.zip` historic data | manual |

**Rule:** if a file in this folder gets `import`-ed by code, it's in the wrong
place — move it back into the proper module.
