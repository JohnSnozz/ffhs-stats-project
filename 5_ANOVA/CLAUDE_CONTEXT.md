# CLAUDE_CONTEXT.md - ANOVA-Analyse

Diese Datei dient als Kontext für Claude Code Sessions zu diesem Thema.

## Was wurde gemacht

ANOVA-Analyse für alle 223 eidgenössischen Abstimmungen (2000-2025) mit drei kategorialen Variablen:

1. **Röstigraben (Sprachgebiete)**: DE, FR, IT, RM
2. **Stadt-Land**: Städtisch, Intermediär, Ländlich
3. **Grossregionen**: 7 Schweizer Grossregionen

### Ergebnisse

| Kategorie | Signifikant | Mittlere η² | Max η² |
|-----------|-------------|-------------|--------|
| Röstigraben | 99.1% | 0.285 | 0.780 |
| Stadt-Land | 86.1% | 0.044 | 0.239 |
| Grossregionen | 100% | 0.256 | 0.683 |

**Haupterkenntnis**: Der Röstigraben ist statistisch messbar und erklärt durchschnittlich 29% der Varianz im Abstimmungsverhalten.

## Erstellte Dateien

```
5_ANOVA/
├── anova_analysis.py              # Hauptscript: ANOVA für alle 223 Abstimmungen
├── anova_visualizations.py        # Visualisierungen generieren
├── anova_notebook.ipynb           # Jupyter Notebook mit 5 Beispielen
├── anova_results_full.csv         # Alle 669 ANOVA-Ergebnisse (223 × 3)
├── anova_summary.csv              # Zusammenfassung nach Kategorie
├── top15_roestigraben_sprachgebiete.csv
├── top15_stadt-land.csv
├── top15_grossregionen.csv
├── fig_example1_ernaehrung.png    # Boxplot Ernährungssouveränität
├── fig_example2_organspende.png   # Boxplot Organspende
├── fig_example3_massentierhaltung.png
├── fig_example4_liegenschaften.png
├── fig_example5_co2.png           # Kombinierte Analyse
├── fig_effect_distribution.png    # Histogramme der Effektstärken
└── CLAUDE_CONTEXT.md              # Diese Datei

Basisordner:
├── anova_report.html              # HTML-Report mit Code-Erklärungen
```

## Datenbank-Zugriff

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/processed/swiss_votings.db')

# Abstimmungsdaten mit Features
query = """
SELECT
    v.municipality_id,
    v.proposal_id,
    v.title_de,
    v.ja_prozent,
    mf.sprachgebiete,
    mf.staedtische_laendliche_gebiete,
    mf.grossregionen_der_schweiz
FROM v_voting_results_analysis v
INNER JOIN municipality_features mf ON v.municipality_id = mf.bfs_nr
WHERE v.ja_prozent IS NOT NULL
"""
df = pd.read_sql_query(query, conn)
```

## Labels für Kategorien

```python
# Sprachgebiete
sprach_map = {1: 'Deutsch', 2: 'Französisch', 3: 'Italienisch', 4: 'Rätoromanisch'}

# Stadt-Land
stadt_map = {1: 'Städtisch', 2: 'Intermediär', 3: 'Ländlich'}

# Grossregionen
region_map = {
    1: 'Région lémanique', 2: 'Espace Mittelland', 3: 'Nordwestschweiz',
    4: 'Zürich', 5: 'Ostschweiz', 6: 'Zentralschweiz', 7: 'Ticino'
}
```

## Wichtige Proposal-IDs (Top-Effekte)

| ID | Abstimmung | Jahr | Effekt |
|----|------------|------|--------|
| 166 | Ernährungssouveränität | 2018 | Röstigraben η²=0.78 |
| 200 | Organspende-Gesetz | 2022 | Röstigraben η²=0.78 |
| 202 | Massentierhaltung | 2022 | Stadt-Land η²=0.24 |
| 222 | Liegenschaftssteuern | 2025 | Grossregionen η²=0.68 |
| 188 | CO2-Gesetz | 2021 | Stadt-Land η²=0.23 |

## Mögliche Erweiterungen

- [ ] Post-hoc Tests (Tukey HSD) für paarweise Vergleiche
- [ ] Zwei-Wege-ANOVA (Interaktion Sprache × Stadt-Land)
- [ ] Zeitliche Entwicklung der Effektstärken
- [ ] Verknüpfung mit Cluster-Analyse (4_Cluster)
- [ ] Thematische Gruppierung der Abstimmungen

## Instruktion für neue Claude-Session

```
Ich arbeite an der ANOVA-Analyse für Schweizer Abstimmungsdaten.
Lies zuerst 5_ANOVA/CLAUDE_CONTEXT.md für den aktuellen Stand.
Die Datenbank ist data/processed/swiss_votings.db (nur lesen).
Alle Outputs in den Ordner 5_ANOVA schreiben.
```

## Technische Notizen

- matplotlib backend auf 'Agg' setzen für headless (WSL)
- 38 Gemeinden (~1.8%) ohne Feature-Zuordnung (Fusionen, Auslandschweizer)
- η² (Eta-Quadrat) als Effektstärke: 0.01 klein, 0.06 mittel, 0.14+ gross
- scipy.stats.f_oneway() für ANOVA
