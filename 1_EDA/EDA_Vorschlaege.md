# EDA Vorschlaege

Basierend auf den verfuegbaren Daten (223 Vorlagen, 2121 Gemeinden, 2000-2025).

---

## A) Voting Data EDA

### A1. Verteilung der Zustimmungsraten
- Histogramm `ja_prozent` ueber alle Abstimmungen
- Boxplots pro Vorlage (Streuung zwischen Gemeinden)
- Extremwerte: Welche Vorlagen polarisieren am staerksten?

### A2. Stimmbeteiligung
- Histogramm `stimmbeteiligung` (Gesamtverteilung)
- Zeitlicher Trend der Stimmbeteiligung (Liniendiagramm)
- Korrelation: Stimmbeteiligung vs. Zustimmung?

### A3. Zeitliche Muster
- Anzahl Vorlagen pro Jahr
- Trend der durchschnittlichen Zustimmung ueber Zeit
- Saisonalitaet? (z.B. Fruehling vs. Herbst)

### A4. Geografische Uebersicht
- Durchschnittliche Zustimmung pro Kanton (Heatmap/Barplot)
- Kantonale Unterschiede in der Stimmbeteiligung
- Streuung innerhalb vs. zwischen Kantonen

---

## B) Features (Gemeindemerkmale) EDA

### B1. Kategorische Verteilungen
- Sprachregionen: Wie viele Gemeinden pro Sprache? (de/fr/it/rm)
- Stadt/Land: Verteilung `staedtische_laendliche_gebiete`
- Gemeindetypologie (9 Typen): Balkendiagramm

### B2. Urbanisierung
- Grossregionen (7): Anzahl Gemeinden pro Region
- Agglomerationsgroesse: Verteilung der 5 Klassen
- Metropolitanregionen: Zuordnung der Gemeinden

### B3. Feature-Beziehungen
- Kreuztabelle: Sprachgebiet x Stadt/Land
- Kreuztabelle: Grossregion x Gemeindetypologie
- Mosaikplot fuer kategorische Zusammenhaenge

---

## Empfohlene Reihenfolge

| Nr | Notebook | Fokus |
|----|----------|-------|
| 1 | `1a_voting_distributions.ipynb` | A1 + A2 (Zustimmung & Beteiligung) |
| 2 | `1b_voting_temporal.ipynb` | A3 (Zeitreihen) |
| 3 | `1c_features_overview.ipynb` | B1 + B2 (Gemeindemerkmale) |

---

## Datenquellen

```python
# Voting-Daten
SELECT voting_date, proposal_id, title_de, municipality_id,
       ja_prozent, stimmbeteiligung
FROM v_voting_results_analysis

# Feature-Daten
SELECT * FROM v_municipality_features_labeled
```
