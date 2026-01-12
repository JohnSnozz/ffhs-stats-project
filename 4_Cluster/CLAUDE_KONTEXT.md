# Claude Kontext: PCA und Cluster-Analyse

Letzte Bearbeitung: 2026-01-05

## Zusammenfassung

Analyse von Schweizer Abstimmungsdaten (2000-2025) mit PCA und Cluster-Analyse.
- 2109 Gemeinden
- 223 Abstimmungsvorlagen
- 3 Hauptkomponenten (59% erklärte Varianz)
- 4 Cluster identifiziert

## Achsen-Interpretation (WICHTIG!)

Die Achsen wurden mehrfach korrigiert. Aktuelle korrekte Interpretation:

| Achse | Negativ (-) | Positiv (+) | Beispiele |
|-------|-------------|-------------|-----------|
| **PC1** | Rechts | Links | Muotathal (-26.5) vs Lausanne (+25.2) |
| **PC2** | Konservativ | Liberal | Muotathal (-8.0) vs Zug (+10.1) |
| **PC3** | Technokratisch | Ökologisch | Muotathal (-7.7) vs Bern (+17.1) |

**WICHTIG für Grafiken:**
- X-Achse (PC1) ist INVERTIERT: Links erscheint links, Rechts erscheint rechts
- `ax.invert_xaxis()` wird verwendet

## Cluster-Namen

| Cluster | Name | PC1 | PC2 | Anzahl | Typische Gemeinden |
|---------|------|-----|-----|--------|-------------------|
| 0 | Rechts-Konservativ | -9.4 | -1.2 | 683 (32%) | Muotathal, Schwyz, Appenzell |
| 1 | Links-Konservativ | +6.7 | -7.9 | 330 (16%) | Genève, Lugano, Nods |
| 2 | Mitte-Liberal | -0.4 | +4.2 | 719 (34%) | Zürich, Luzern, Zug, St. Gallen |
| 3 | Links-Liberal | +12.0 | +1.1 | 377 (18%) | Bern, Lausanne, Fribourg |

## Wichtige Städte im PCA-Raum

```
Zürich:     PC1=+16.2 (Links)    PC2=+4.7 (Liberal)   PC3=+15.3 (Öko)     → Mitte-Liberal
Genève:     PC1=+20.8 (Links)    PC2=-5.1 (Kons.)     PC3=+8.9 (Öko)      → Links-Konservativ
Bern:       PC1=+21.0 (Links)    PC2=+4.5 (Liberal)   PC3=+17.1 (Öko)     → Links-Liberal
Lausanne:   PC1=+25.2 (Links)    PC2=-3.6 (Kons.)     PC3=+7.9 (Öko)      → Links-Liberal
Muotathal:  PC1=-26.5 (Rechts)   PC2=-8.0 (Kons.)     PC3=-7.7 (Techn.)   → Rechts-Konservativ
Zug:        PC1=+2.3  (Mitte)    PC2=+10.1 (Liberal)  PC3=+3.2 (Öko)      → Mitte-Liberal
```

## Dateien

### Jupyter Notebooks
- `3_PCA/pca_analysis.ipynb` - PCA-Analyse
- `4_Cluster/cluster_analysis.ipynb` - Clustering auf Rohdaten (Silhouette 0.167)
- `4_Cluster/cluster_on_pca.ipynb` - Clustering auf PCA-Scores (Silhouette 0.335)
- `4_Cluster/cluster_visualization.ipynb` - Visualisierungen mit Labels

### Daten
- `4_Cluster/municipality_clusters_pca.csv` - Gemeinden mit Cluster-Zuordnung und PCA-Scores
- `data/processed/swiss_votings.db` - SQLite-Datenbank

### Dokumentation
- `analyse_dokumentation.html` - Hauptdokumentation (HTML)
- `4_Cluster/cluster_visualization.html` - Notebook als HTML exportiert

### Generierte Grafiken (4_Cluster/)
- `cluster_pc1_pc2.png` - Links-Rechts vs Konservativ-Liberal
- `cluster_pc1_pc3.png` - Links-Rechts vs Technokratisch-Ökologisch
- `cluster_pc2_pc3.png` - Konservativ-Liberal vs Technokratisch-Ökologisch
- `cluster_overview.png` - Alle drei Ansichten
- `cluster_3d.png` - 3D-Visualisierung

## Offene Punkte / Zu verifizieren

1. **Cluster 1 "Links-Konservativ"** enthält Genève und Lausanne mit negativen PC2-Werten
   - Ist "Konservativ" hier die richtige Bezeichnung für PC2-?
   - Evtl. passt "Romandie-typisch" oder "Westschweizer" besser?

2. **Silhouette Score** von 0.335 ist akzeptabel, aber nicht hervorragend
   - Mögliche Verbesserung: Andere Clustering-Methoden testen

3. **Achsen-Validierung**: User sollte prüfen ob die Achsen nun korrekt sind
   anhand der beschrifteten Gemeinden in den Plots

## Befehle zum Fortsetzen

```bash
# Notebook ausführen
cd /home/jonas/ffhs-stats-project-local/4_Cluster
jupyter nbconvert --to notebook --execute cluster_visualization.ipynb --output cluster_visualization_executed.ipynb

# HTML exportieren
jupyter nbconvert --to html cluster_visualization_executed.ipynb --output cluster_visualization.html

# Daten laden (Python)
import pandas as pd
df = pd.read_csv('municipality_clusters_pca.csv')
```

## Referenz-Paper

Hermann & Leuthold - Dimensionen des Schweizer politischen Raums:
- Dimension 1: Links-Rechts (wirtschaftlich)
- Dimension 2: Konservativ-Liberal (gesellschaftlich)
- Dimension 3: Ökologisch-Technokratisch (halbe Dimension)
