# Validation Folder

This folder contains scripts and data files for validating the municipal merger handling in the database.

## Scripts

### `validate_merger_data.py`
Creates detailed validation reports comparing original voting data with aggregated data after applying municipal mergers.

**Usage:**
```bash
cd validation
python validate_merger_data.py
```

**What it does:**
- Selects municipalities with most complex merger histories (top 10) plus 3 simple cases
- Extracts original voting data (individual municipalities)
- Extracts aggregated voting data (merged municipalities)
- Compares the two to verify perfect matching
- Generates 5 CSV files for manual verification

### `test_merger_views.py`
Runs tests on the merger views to demonstrate how they work.

**Usage:**
```bash
cd validation
python test_merger_views.py
```

## Output Files

All CSV files are generated in this folder:

1. **`merger_validation_summary.csv`**
   - Overview of selected municipalities
   - Shows predecessor count and merge depth

2. **`merger_validation_timeline.csv`**
   - Timeline of all mergers affecting selected municipalities
   - Shows mutation dates and types

3. **`merger_validation_original.csv`**
   - Original voting data (before mergers)
   - Each municipality shown separately
   - Example: 5226, 5236, 5237 as separate entries

4. **`merger_validation_aggregated.csv`**
   - Aggregated voting data (after mergers)
   - Municipalities combined according to merger logic
   - Example: 5226 + 5236 + 5237 = Capriasca (5226)

5. **`merger_validation_comparison.csv`**
   - Side-by-side comparison for validation
   - Shows: aggregated_ja vs original_ja_sum
   - Includes match flags (True/False)

## Validation Results

**Current Status:**
- ✅ 100% perfect match for all non-merged municipalities
- ✅ Capriasca (BFS 5226) correctly aggregates 3 municipalities
- ✅ All vote totals match exactly when summed correctly

## How to Use for Verification

1. Run `validate_merger_data.py` to generate fresh validation files
2. Open `merger_validation_comparison.csv` in Excel/LibreOffice
3. For each row, verify:
   - `aggregated_ja` = `original_ja_sum`
   - `aggregated_nein` = `original_nein_sum`
   - `ja_match`, `nein_match`, `gueltig_match` should all be TRUE
4. Cross-reference with `merger_validation_original.csv` to see individual municipalities
5. Check `merger_validation_timeline.csv` to understand when mergers happened

## Example: Capriasca Verification

Capriasca (BFS 5226) is the main example of municipal aggregation in our dataset:

**Original municipalities (2000-2008):**
- 5226 (Capriasca): 1009 ja votes in first voting
- 5236 (Collina d'Oro): 814 ja votes in first voting
- 5237 (Alto Malcantone): 268 ja votes in first voting
- **Sum: 2091 ja votes**

**Aggregated (analysis-ready):**
- 5226 (Capriasca): 2091 ja votes in first voting
- **Perfect match!** ✅

**Merger timeline:**
- 2001-2005: Multiple small municipalities merge into 5226, 5236, 5237
- 2004: BFS renumbering (5236→5392, 5237→5393)
- 2008-04-20: Final merger (5392 + 5393 → 5226)
- 2008+: Only 5226 appears in voting data

## Notes

- Logs are written to `../logs/validate_mergers_YYYYMMDD_HHMMSS.log`
- Database connection is to `../data/swiss_votings.db`
- Scripts are designed to be run from this validation folder
- All paths are relative to ensure files end up in the correct locations
