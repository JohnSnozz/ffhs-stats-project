# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Project Overview

FFHS Semesterarbeit: Statistical analysis of Swiss federal voting data (eidgenössische Abstimmungen) 2000-2025.

**Goal**: Apply statistical methods (EDA, Regression, PCA, Clustering) to analyze voting patterns across ~2,100 Swiss municipalities over 25 years.

## Project Structure

```
ffhs-stats-project-local/
├── data/
│   ├── raw/                      # Original data sources
│   │   ├── votes/                # 78 JSON voting files
│   │   ├── features/             # Additional feature data
│   │   └── Mutierte_Gemeinden.xlsx
│   ├── processed/
│   │   └── swiss_votings.db      # SQLite database (analysis-ready)
│   └── exports/                  # CSV exports
├── scripts/                      # Data processing scripts
│   ├── import_all_data.py        # Main import script
│   ├── create_analysis_views.py  # Creates merger-corrected views
│   └── export_data.py            # CSV export
├── notebooks/                    # Analysis notebooks (to be created)
├── docs/                         # Reference materials
│   ├── Projektanforderungen/     # FFHS requirements
│   ├── papers/                   # Reference papers
│   ├── beispiele/                # Example semester papers
│   └── vorgaben/                 # Templates
├── output/                       # Generated plots/results
└── CLAUDE.md
```

## Data Summary

| Metric | Value |
|--------|-------|
| Votings | 78 |
| Proposals | 223 |
| Municipalities | 2,121 (after merger correction) |
| Time Period | 2000-03-12 to 2025-09-28 |
| Database | `data/processed/swiss_votings.db` |

## Working with the Database

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/processed/swiss_votings.db')

# USE THIS VIEW for all analysis (handles municipal mergers correctly)
df = pd.read_sql_query("""
    SELECT
        municipality_id, municipality_name,
        voting_date, proposal_id, title_de,
        ja_stimmen_absolut, nein_stimmen_absolut,
        ja_prozent, stimmbeteiligung
    FROM v_voting_results_analysis
""", conn)
```

## Key Database Views

- **`v_voting_results_analysis`** - Main analysis view with merger-corrected data
- **`v_stable_municipality_mapping`** - Maps historical BFS numbers to current
- **`v_municipality_data_quality`** - Shows which municipalities were aggregated

## Scripts Usage

```bash
# If you need to reimport data:
python scripts/import_all_data.py
python scripts/create_analysis_views.py

# To export CSV:
python scripts/export_data.py
```

## Semester Paper Requirements

Based on `docs/Projektanforderungen/`:

1. **EDA** - Explorative Datenanalyse
2. **Multiple Lineare Regression**
3. **Logistische Regression**
4. **ANOVA / Hypothesentest**
5. **PCA / Faktoranalyse**
6. **Clusteranalyse**
7. Fazit + Literaturverzeichnis
8. Eigenständigkeitserklärung + Hilfsmittelverzeichnis (KI-Nutzung deklarieren!)

## Notes

- Municipal mergers are handled automatically in `v_voting_results_analysis`
- Only Capriasca (BFS 5226) shows aggregation in our data period
- Use Python/Jupyter notebooks for analysis
- Output plots to `output/` folder
