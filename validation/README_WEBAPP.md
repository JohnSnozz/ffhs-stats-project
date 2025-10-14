# Voting Data Validation Web App

This is a web-based validation tool that allows you to compare aggregated database results with original JSON voting data for Swiss municipalities that have undergone mergers.

## Purpose

The app helps verify that the municipal merger aggregation logic is working correctly by:
1. Showing aggregated voting results from the database (analysis-ready structure)
2. Displaying merger information from the municipal_changes table
3. Showing original voting results from JSON files (before aggregation)
4. Comparing the aggregated totals with the sum of original data

## Technology Stack

- **Backend**: Bun (TypeScript server)
- **Database**: SQLite with built-in Bun support
- **Frontend**: Vanilla JavaScript, HTML, CSS
- **Data Sources**:
  - Database: `v_voting_results_analysis` view for aggregated data
  - JSON files: Original voting data from `../data/votes/`

## Project Structure

```
validation/
├── server.ts              # Bun server with API endpoints
├── package.json           # Bun project configuration
├── public/                # Frontend files
│   ├── index.html        # Main UI
│   ├── app.js           # Frontend JavaScript
│   └── style.css        # Styling
└── README_WEBAPP.md      # This file
```

## Installation & Running

### Prerequisites
- Bun installed (already available on your system)
- SQLite database with analysis views created
- JSON voting files in `../data/votes/`

### Start the Server

```bash
cd validation
bun run server.ts
```

Or use the npm script:
```bash
bun run start
```

For development with auto-reload:
```bash
bun run dev
```

The server will start at: **http://localhost:3000**

## How to Use

1. **Start the server** (see above)

2. **Open your browser** and navigate to http://localhost:3000

3. **Select a voting date** from the first dropdown
   - Shows all 78 votings from 2000-2025
   - Displays the number of proposals for each voting

4. **Select a municipality** from the second dropdown
   - Only shows municipalities that had mergers
   - Displays the number of source municipalities aggregated

5. **Click "Compare Results"** to load the comparison

6. **Review the three panels:**

   **LEFT - Aggregated Results (Database)**
   - Shows voting results from `v_voting_results_analysis` view
   - This is the "current structure" with mergers applied
   - Displays source municipalities if aggregated (e.g., "5226,5236,5237")

   **MIDDLE - Merger Information**
   - Shows all predecessor municipalities that merged
   - Displays merger dates and types
   - Helps understand the merger timeline

   **RIGHT - Original Results (JSON)**
   - Shows voting results from original JSON files
   - Displays each predecessor municipality separately
   - Shows totals if multiple municipalities exist

7. **Check the validation summary** at the bottom
   - ✅ Perfect Match: Aggregated = Sum of Original
   - ❌ Mismatch: Shows differences that need investigation

## API Endpoints

### GET /api/votings
Returns all voting dates with proposal counts.

**Response:**
```json
[
  {
    "voting_date": "20190519",
    "proposal_count": 2,
    "proposals": "Proposal 1 | Proposal 2"
  }
]
```

### GET /api/municipalities
Returns municipalities that had mergers.

**Response:**
```json
[
  {
    "municipality_id": "5226",
    "municipality_name": "Capriasca",
    "source_count": 3,
    "voting_count": 78
  }
]
```

### GET /api/compare/:votingDate/:municipalityId
Compares aggregated vs original data for a specific voting and municipality.

**Example:** `/api/compare/20190519/5226`

**Response:**
```json
{
  "voting_date": "20190519",
  "municipality_id": "5226",
  "aggregated": [
    {
      "municipality_id": "5226",
      "municipality_name": "Capriasca",
      "proposal_id": 171,
      "title_de": "...",
      "ja_stimmen_absolut": 2681,
      "nein_stimmen_absolut": 1487,
      "gueltige_stimmen": 4168,
      "source_municipality_count": 3,
      "source_bfs_numbers": "5226,5236,5237"
    }
  ],
  "original": {
    "predecessors": [...],
    "results": [
      {
        "vorlage_id": 6270,
        "title_de": "...",
        "municipalities": [
          {
            "municipality_id": "5226",
            "municipality_name": "...",
            "ja_stimmen_absolut": 1009,
            "nein_stimmen_absolut": 520,
            "gueltige_stimmen": 1529
          },
          ...
        ]
      }
    ]
  }
}
```

## Example Use Case: Capriasca (5226)

**Background:**
- Capriasca (5226) was formed from 3 municipalities in 2008
- Original municipalities: 5226, 5236, 5237
- For votings before 2008, these appear as 3 separate entries
- For votings after 2008, only 5226 appears

**Steps:**
1. Select voting: 2019-05-19 (after merger)
2. Select municipality: Capriasca (5226)
3. Compare:
   - **Left**: Shows aggregated total (e.g., 2681 Ja votes)
   - **Middle**: Shows 3 predecessor municipalities
   - **Right**: Shows individual results that sum to the total
   - **Validation**: Should show ✅ Perfect Match

## Troubleshooting

### Server won't start
- Check if port 3000 is already in use
- Verify the database path is correct: `../data/swiss_votings.db`
- Ensure JSON files exist in `../data/votes/`

### No municipalities shown
- Make sure the analysis views are created: `python create_analysis_ready_views.py`
- Check that `v_voting_results_analysis` view exists in the database

### Aggregated results empty
- Verify the selected municipality has data for the selected voting date
- Check that the municipality ID exists in `v_voting_results_analysis`

### Original results empty
- Verify the JSON file exists for that voting date
- Check that the municipality ID appears in the JSON file
- Note: Municipalities might not appear if they merged before that voting date

## Technical Details

### Database View Used
The app uses `v_voting_results_analysis` which:
- Aggregates voting results to the current municipal structure
- Handles merger chains correctly
- Provides `source_bfs_numbers` for transparency
- Ensures perfect data matching (verified at 100%)

### JSON Structure
The app navigates the nested JSON structure:
```
schweiz.vorlagen[].kantone[].gemeinden[]
```

Municipalities are nested under cantons, not directly under proposals!

### Validation Logic
The comparison sums up the original results for all predecessor municipalities and compares with the aggregated database value. A perfect match means:
```
aggregated.ja_stimmen_absolut === sum(original.municipalities[*].ja_stimmen_absolut)
```

## Future Enhancements

Possible improvements:
- [ ] Export comparison results to CSV
- [ ] Filter by canton or region
- [ ] Show voting trends over time
- [ ] Highlight municipalities with mismatches
- [ ] Add search functionality
- [ ] Display maps with municipality boundaries

## Support

For issues or questions about the validation app:
1. Check the logs in `../logs/` folder
2. Verify the database structure with `sqlite3 ../data/swiss_votings.db .schema`
3. Review the main project documentation in `../CLAUDE.md`

---

**Last Updated**: 2025-10-14
**Version**: 1.0.0
**Maintainer**: FFHS Stats Project
