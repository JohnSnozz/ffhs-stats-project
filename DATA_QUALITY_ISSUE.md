# CRITICAL DATA QUALITY ISSUE

## Swiss Voting JSON Files Contain Retroactively Aggregated Data

**Date Discovered:** 2025-10-14
**Issue Type:** Data Integrity / Source Data Problem
**Severity:** CRITICAL

## Problem Description

The Swiss federal voting JSON files provided by the official source **already contain aggregated data that has been retroactively updated to match the current municipal structure**. This means the historical voting data does NOT reflect the actual municipal boundaries that existed at the time of the voting.

## Evidence

### Example 1: Buchegg (BFS 2465)

**Actual History:**
- Buchegg (2465) was created on **2014-01-01** through merger of 10 municipalities:
  - Tscheppach (2462)
  - Brügglen (2446)
  - Aetingen (2442)
  - Aetigkofen (2441)
  - Bibern (SO) (2444)
  - Gossliwil (2449)
  - Hessigkofen (2450)
  - Mühledorf (SO) (2458)
  - Küttigkofen (2452)
  - Kyburg-Buchegg (2453)

**What SHOULD be in 2009 JSON (voting from 2009-05-17):**
- 10 separate entries for each predecessor municipality with individual vote counts

**What IS ACTUALLY in 2009 JSON:**
- Only 1 entry: "Buchegg (2465)" with aggregated votes
- The 10 predecessor municipalities (2462, 2446, 2442, 2441, 2444, 2449, 2450, 2458, 2452, 2453) **DO NOT EXIST** in the JSON

**Checked in:** `sd-t-17-02-20090517-eidgAbstimmung.json`

### Example 2: Messen (BFS 2457)

**Actual History:**
- Messen (2457) was created on **2010-01-01** through merger of:
  - Brunnenthal (2447)
  - Balm bei Messen (2443)
  - Oberramsern (2460)

**What IS ACTUALLY in 2002 JSON:**
- Only "Messen (2457)" exists
- The 3 predecessor municipalities **DO NOT EXIST**

## Impact on Project

### What This Means

1. **NO TRUE HISTORICAL DATA EXISTS** in the JSON files
   - The JSON files do not preserve the original voting results as they occurred
   - All data has been retroactively aggregated to match post-2014 (or later) structure

2. **VALIDATION IS IMPOSSIBLE**
   - Cannot validate database aggregation against "original" data
   - Both database and JSON show the same aggregated results
   - The validation web app works correctly, but there's nothing meaningful to validate

3. **MUNICIPALITY CHANGES ARE ALREADY APPLIED**
   - The Swiss government applies all municipal mergers retroactively to historical data
   - This is likely done to maintain consistency with current administrative boundaries
   - But it means the original disaggregated data is permanently lost

### What Can Still Be Done

1. **Database aggregation logic is still valid**
   - Our `v_voting_results_analysis` view correctly aggregates data
   - The merger tracking in `municipal_changes` table is accurate
   - The mapping in `v_stable_municipality_mapping` is correct

2. **Validation shows consistency**
   - PERFECT_MATCH results indicate our database correctly mirrors the JSON structure
   - This confirms our import process works correctly

3. **Analysis can proceed**
   - Statistical analysis can use the aggregated data
   - Temporal comparisons are still valid (comparing voting patterns over time)
   - Just cannot disaggregate to original pre-merger municipalities

## Why Swiss Government Did This

### Likely Reasons

1. **Administrative Simplicity**
   - Easier to maintain one current municipal structure across all years
   - Reduces complexity in data publication and maintenance

2. **Reporting Consistency**
   - All reports use current municipality names and boundaries
   - Avoids confusion about which municipalities existed when

3. **Data Access**
   - Users expect to find their current municipality in historical data
   - Makes data more accessible to non-experts

### Trade-offs

**Pros:**
- Consistent structure across all time periods
- Easier for general public to understand
- No need to track historical boundaries

**Cons:**
- **LOSS OF ORIGINAL DATA** - cannot recover pre-merger voting patterns
- Cannot analyze voting behavior of small municipalities before mergers
- Cannot study merger effects on voting patterns
- Violates principles of historical data preservation

## Recommendations

### For This Project

1. **Accept the limitation**
   - The JSON files are the official source of truth
   - There is no access to disaggregated historical data
   - Database should mirror JSON structure (which it does)

2. **Update documentation**
   - Clearly state that data is retroactively aggregated
   - Remove references to "validating against original data"
   - Rename validation app to "Data Consistency Checker"

3. **Adjust validation web app purpose**
   - Change from "validate aggregation logic" to "verify database-JSON consistency"
   - Useful for confirming import process worked correctly
   - Can identify any discrepancies between database and JSON

4. **Research alternatives**
   - Check if Swiss Federal Statistical Office has archived original data
   - Look for academic datasets that preserved historical boundaries
   - Consider contacting BFS directly about historical disaggregated data

### For Analysis

1. **Be transparent**
   - Always mention in analysis that data is retroactively aggregated
   - Cannot make claims about pre-merger municipality voting behavior
   - Focus on trends at current municipal level

2. **Adjust research questions**
   - Focus on questions that work with aggregated data
   - Avoid questions requiring historical disaggregation
   - Consider canton or district-level analysis where boundaries are more stable

## Files to Update

1. **CLAUDE.md** - Add section on data quality limitations
2. **Validation app README** - Change purpose from validation to consistency checking
3. **Analysis documentation** - Add data limitations section

## External Communication

If publishing results, MUST include:
> "Note: Swiss federal voting data is provided in a format that retroactively applies current municipal boundaries to historical voting results. This means voting data from before municipal mergers reflects aggregated results at the post-merger municipal level, rather than the original pre-merger municipal boundaries. Individual pre-merger municipality voting patterns cannot be recovered from the official data."

## Verification Commands

To verify this issue yourself:

```bash
# Check 2009 voting for Buchegg predecessors
cd data/votes
python3 << 'EOF'
import json
data = json.load(open('sd-t-17-02-20090517-eidgAbstimmung.json'))
vorlage = data['schweiz']['vorlagen'][0]
predecessor_ids = ['2462', '2446', '2442', '2441', '2444', '2449', '2450', '2458', '2452', '2453']
for kanton in vorlage['kantone']:
    for gemeinde in kanton.get('gemeinden', []):
        if gemeinde['geoLevelnummer'] in predecessor_ids:
            print(f"Found: {gemeinde['geoLevelnummer']} {gemeinde['geoLevelname']}")
EOF

# Expected: No output (none of these municipalities exist in 2009 JSON)
```

## Conclusion

This is NOT a bug in our code or database. This is a **fundamental limitation of the source data**. The Swiss government provides only retroactively aggregated voting data, not historical data as it existed at the time of voting.

Our validation web app is working correctly - it just reveals that there's no meaningful validation to perform because both database and JSON contain the same aggregated data.

---

**Status:** CONFIRMED
**Resolution:** Document limitation, adjust project scope
**Action Required:** Update all documentation to reflect this limitation
