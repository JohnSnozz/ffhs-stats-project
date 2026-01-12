# Claude Session Context - FFHS Stats Project

**Letzte Aktualisierung:** 2026-01-05
**Thema:** Statistische Analyse Schweizer Abstimmungsdaten (FFHS Semesterarbeit)

---

## Projekt-Überblick

Analyse von Schweizer Bundesabstimmungen (2000-2025) auf Gemeindeebene mit statistischen Methoden:
- EDA (Explorative Datenanalyse)
- Multiple Lineare Regression (MLR)
- Logistische Regression
- PCA / Faktoranalyse
- Clusteranalyse
- ANOVA

**Datenbank:** `data/processed/swiss_votings.db` (SQLite)
- 78 Abstimmungstermine, 223 Vorlagen
- ~2'100 Gemeinden
- 32 Features pro Gemeinde (Demographie, Wirtschaft, Politik, Raum)

---

## Aktueller Stand

### Abgeschlossene Arbeiten

1. **Datenimport & Aufbereitung**
   - BFS Regionalporträts 2021 importiert
   - ESTV Einkommensdaten 2020 importiert
   - Gemeindefusionen korrekt behandelt
   - 100% Gemeinde-Coverage erreicht

2. **EDA Notebooks**
   - `1_EDA/1a_voting_eda.ipynb` - Abstimmungs-EDA
   - `1_EDA/1b_features_eda.ipynb` - Features-EDA
   - `1_EDA/Votings/2_voting_correlations.ipynb` - Voting-Korrelationen
   - `1_EDA/Features/2_features_correlations.ipynb` - Feature-Korrelationen mit hierarchischem Clustering
   - `1_EDA/3_features_voting_correlations.ipynb` - Feature-Voting-Korrelationen

3. **Regression**
   - `2_Regression/MLR/Korrelationen/1_mlr_individuelle_features.ipynb` - MLR mit individuellen Features pro Abstimmung
   - `2_Regression/MLR/Korrelationen/2_logistische_regression_individuelle_features.ipynb` - Logistische Regression
   - Ergebnisse: R² = 0.65-0.80, AUC = 0.90-0.95

4. **HTML-Bericht**
   - `analyse_bericht.html` - Dokumentation mit Code-Erklärungen und Grafiken
   - Enthält hierarchisches Clustering der Korrelationsmatrix

### Zuletzt bearbeitet

- Hierarchisches Clustering (Ward-Methode) zur Korrelationsmatrix hinzugefügt
- `sns.clustermap` mit Dendrogrammen implementiert
- HTML-Bericht mit neuem Abschnitt 2.3 aktualisiert

---

## Wichtige Dateien

```
ffhs-stats-project-local/
├── CLAUDE.md                    # Projekt-Instruktionen für Claude
├── CLAUDE_SESSION_CONTEXT.md    # Diese Datei (Session-Kontext)
├── analyse_bericht.html         # HTML-Dokumentation
├── data/
│   └── processed/
│       └── swiss_votings.db     # Hauptdatenbank
├── 1_EDA/
│   ├── Features/
│   │   ├── 2_features_correlations.ipynb
│   │   ├── correlation_features_full.png
│   │   └── correlation_features_clustered.png  # NEU
│   └── Votings/
│       └── 2_voting_correlations.ipynb
├── 2_Regression/
│   └── MLR/Korrelationen/
│       ├── 1_mlr_individuelle_features.ipynb
│       ├── 2_logistische_regression_individuelle_features.ipynb
│       └── output/
│           ├── mlr_individual_features_results.txt
│           └── logreg_individual_features_results.txt
├── 3_PCA/
├── 4_Cluster/
└── 5_ANOVA/
```

---

## Technische Details

### Datenbank-Abfrage (Beispiel)
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/processed/swiss_votings.db')

# Abstimmungsdaten mit Features
df = pd.read_sql_query("""
    SELECT
        CAST(vr.geo_id AS INTEGER) as bfs_nr,
        vr.geo_name,
        v.voting_date,
        p.title_de,
        vr.ja_stimmen_prozent as ja_prozent
    FROM voting_results vr
    JOIN proposals p ON vr.proposal_id = p.proposal_id
    JOIN votings v ON p.voting_id = v.voting_id
    WHERE CAST(vr.geo_id AS INTEGER) BETWEEN 100 AND 9000
""", conn)

# Features
df_features = pd.read_sql_query("""
    SELECT * FROM municipality_features_complete
""", conn)
```

### Wichtige Tabellen
- `voting_results` - Abstimmungsergebnisse pro Gemeinde
- `proposals` - Vorlagen-Metadaten
- `votings` - Abstimmungstermine
- `municipality_features_complete` - Alle Gemeindemerkmale

---

## Offene Aufgaben / Nächste Schritte

- [ ] PCA-Analyse vervollständigen
- [ ] Clusteranalyse der Gemeinden
- [ ] ANOVA für Gruppenvergleiche
- [ ] Fazit und Interpretation schreiben
- [ ] Literaturverzeichnis erstellen
- [ ] Eigenständigkeitserklärung (KI-Nutzung deklarieren!)

---

## Hinweise für Fortsetzung

1. **CLAUDE.md lesen** - Enthält Projekt-Struktur und DB-Schema
2. **analyse_bericht.html** - Gibt Überblick über bisherige Analysen
3. **Output-Ordner prüfen** - Ergebnisse der Regressionen als .txt und .png

Bei Fragen zum Kontext: Diese Datei und CLAUDE.md konsultieren.
