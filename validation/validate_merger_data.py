#!/usr/bin/env python3
"""
Validate merger data by extracting detailed information for municipalities
with the most complex merger histories.

Creates two CSV files:
1. merger_validation_summary.csv - Overview of selected municipalities
2. merger_validation_detailed.csv - Complete voting history for validation

This allows manual cross-referencing between:
- Original voting data (before merger)
- Aggregated data (after merger, from views)
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import sys
import json

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path('../logs')
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'validate_mergers_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def find_most_complex_mergers(conn, logger):
    """Find municipalities with most complex merger histories"""
    logger.info("Finding municipalities with most mergers...")

    query = """
    WITH merger_complexity AS (
        -- Count how many municipalities merged into each analysis municipality
        SELECT
            m.analysis_bfs as current_bfs,
            m.analysis_name as current_name,
            COUNT(DISTINCT m.original_bfs) as predecessor_count,
            MAX(m.merge_depth) as max_merge_depth,
            GROUP_CONCAT(DISTINCT m.original_bfs) as all_predecessors
        FROM v_stable_municipality_mapping m
        WHERE m.merge_depth > 0
        GROUP BY m.analysis_bfs, m.analysis_name
    ),
    voting_participation AS (
        -- Count how many votings each municipality participated in
        SELECT
            geo_id,
            COUNT(DISTINCT voting_id) as voting_count
        FROM voting_results
        WHERE geo_level = 'municipality'
        GROUP BY geo_id
    )
    SELECT
        mc.current_bfs,
        mc.current_name,
        mc.predecessor_count,
        mc.max_merge_depth,
        mc.all_predecessors,
        COALESCE(MAX(vp.voting_count), 0) as max_voting_count
    FROM merger_complexity mc
    LEFT JOIN voting_participation vp ON mc.all_predecessors LIKE '%' || vp.geo_id || '%'
    GROUP BY mc.current_bfs, mc.current_name, mc.predecessor_count, mc.max_merge_depth, mc.all_predecessors
    ORDER BY mc.predecessor_count DESC, mc.max_merge_depth DESC
    """

    df = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(df)} municipalities with mergers")

    return df

def find_simple_cases(conn, logger):
    """Find municipalities with no mergers (simple cases)"""
    logger.info("Finding simple cases (no mergers)...")

    query = """
    SELECT
        m.original_bfs as current_bfs,
        m.original_name as current_name,
        1 as predecessor_count,
        0 as max_merge_depth,
        m.original_bfs as all_predecessors,
        COUNT(DISTINCT vr.voting_id) as voting_count
    FROM v_stable_municipality_mapping m
    INNER JOIN voting_results vr ON m.original_bfs = vr.geo_id
    WHERE m.merge_depth = 0
    AND vr.geo_level = 'municipality'
    GROUP BY m.original_bfs, m.original_name
    ORDER BY voting_count DESC
    LIMIT 3
    """

    df = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(df)} simple cases")

    return df

def get_merger_timeline(conn, bfs_list, logger):
    """Get detailed merger timeline for specific municipalities"""
    logger.info("Getting merger timeline...")

    placeholders = ','.join(['?' for _ in bfs_list])

    query = f"""
    SELECT
        me.old_bfs_number,
        me.old_name,
        me.new_bfs_number,
        me.new_name,
        me.mutation_date,
        me.change_category,
        me.municipalities_in_merger,
        me.was_in_voting_data
    FROM v_municipality_evolution me
    WHERE me.old_bfs_number IN ({placeholders})
    OR me.new_bfs_number IN ({placeholders})
    ORDER BY me.mutation_date, me.new_bfs_number
    """

    df = pd.read_sql_query(query, conn, params=bfs_list + bfs_list)

    return df

def get_original_voting_data(conn, bfs_list, logger):
    """Get original voting data for specific municipalities (before mergers)"""
    logger.info("Getting original voting data...")

    placeholders = ','.join(['?' for _ in bfs_list])

    query = f"""
    SELECT
        vr.geo_id as municipality_bfs,
        vr.geo_name as municipality_name,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        vr.ja_stimmen_absolut,
        vr.nein_stimmen_absolut,
        vr.gueltige_stimmen,
        vr.ja_stimmen_prozent,
        vr.stimmbeteiligung_prozent
    FROM voting_results vr
    INNER JOIN votings v ON vr.voting_id = v.voting_id
    INNER JOIN proposals p ON vr.proposal_id = p.proposal_id
    WHERE vr.geo_level = 'municipality'
    AND vr.geo_id IN ({placeholders})
    ORDER BY vr.geo_id, v.voting_date, p.proposal_id
    """

    df = pd.read_sql_query(query, conn, params=bfs_list)
    logger.info(f"Found {len(df)} original voting records")

    return df

def get_aggregated_voting_data(conn, current_bfs_list, logger):
    """Get aggregated voting data (after mergers) from views"""
    logger.info("Getting aggregated voting data from views...")

    placeholders = ','.join(['?' for _ in current_bfs_list])

    query = f"""
    SELECT
        municipality_id as current_bfs,
        municipality_name as current_name,
        voting_date,
        proposal_id,
        title_de,
        ja_stimmen_absolut,
        nein_stimmen_absolut,
        gueltige_stimmen,
        ja_prozent,
        stimmbeteiligung,
        merged_municipality_count,
        original_bfs_numbers
    FROM v_voting_results_current
    WHERE municipality_id IN ({placeholders})
    ORDER BY municipality_id, voting_date, proposal_id
    """

    df = pd.read_sql_query(query, conn, params=current_bfs_list)
    logger.info(f"Found {len(df)} aggregated voting records")

    return df

def get_voting_from_json(voting_date, bfs_number, logger):
    """Get voting data directly from JSON file for validation"""
    logger.debug(f"Looking for voting {voting_date}, BFS {bfs_number} in JSON files...")

    # Find the JSON file for this voting date
    json_pattern = f"../data/votes/*{voting_date}*.json"
    json_files = list(Path('..').glob(json_pattern))

    if not json_files:
        logger.warning(f"No JSON file found for voting date {voting_date}")
        return None

    json_file = json_files[0]

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Navigate through the JSON structure to find the municipality
        schweiz = data.get('schweiz', {})
        vorlagen = schweiz.get('vorlagen', [])

        for vorlage in vorlagen:
            kantone = vorlage.get('kantone', [])
            for kanton in kantone:
                gemeinden = kanton.get('gemeinden', [])
                for gemeinde in gemeinden:
                    if str(gemeinde.get('geoLevelnummer')) == str(bfs_number):
                        resultat = gemeinde.get('resultat', {})
                        return {
                            'bfs': bfs_number,
                            'name': gemeinde.get('geoLevelname'),
                            'ja_absolut': resultat.get('jaStimmenAbsolut'),
                            'nein_absolut': resultat.get('neinStimmenAbsolut'),
                            'ja_prozent': resultat.get('jaStimmenInProzent')
                        }

        return None

    except Exception as e:
        logger.error(f"Error reading JSON file {json_file}: {e}")
        return None

def create_validation_report(conn, selected_municipalities, logger):
    """Create detailed validation report"""
    logger.info("Creating validation report...")

    # Get all predecessor BFS numbers
    all_predecessors = []
    current_bfs_list = []

    for _, row in selected_municipalities.iterrows():
        current_bfs_list.append(row['current_bfs'])
        predecessors = str(row['all_predecessors']).split(',')
        all_predecessors.extend(predecessors)

    all_predecessors = list(set(all_predecessors))

    logger.info(f"Analyzing {len(all_predecessors)} predecessor municipalities")
    logger.info(f"Mapping to {len(current_bfs_list)} current municipalities")

    # Get merger timeline
    timeline_df = get_merger_timeline(conn, all_predecessors, logger)

    # Get original data
    original_df = get_original_voting_data(conn, all_predecessors, logger)

    # Get aggregated data
    aggregated_df = get_aggregated_voting_data(conn, current_bfs_list, logger)

    # Add validation columns to show differences
    validation_records = []

    for _, row in aggregated_df.iterrows():
        current_bfs = row['current_bfs']
        voting_date = row['voting_date']
        proposal_id = row['proposal_id']
        original_bfs_list = str(row['original_bfs_numbers']).split(',')

        # Find matching original records
        original_records = original_df[
            (original_df['municipality_bfs'].isin(original_bfs_list)) &
            (original_df['voting_date'] == voting_date) &
            (original_df['proposal_id'] == proposal_id)
        ]

        # Calculate sums from original data
        if len(original_records) > 0:
            original_ja_sum = original_records['ja_stimmen_absolut'].sum()
            original_nein_sum = original_records['nein_stimmen_absolut'].sum()
            original_gueltig_sum = original_records['gueltige_stimmen'].sum()

            validation_records.append({
                'current_bfs': current_bfs,
                'current_name': row['current_name'],
                'voting_date': voting_date,
                'proposal_id': proposal_id,
                'title_de': row['title_de'],
                'merged_count': row['merged_municipality_count'],
                'original_bfs_numbers': row['original_bfs_numbers'],
                # Aggregated data (from view)
                'aggregated_ja': row['ja_stimmen_absolut'],
                'aggregated_nein': row['nein_stimmen_absolut'],
                'aggregated_gueltig': row['gueltige_stimmen'],
                'aggregated_ja_pct': row['ja_prozent'],
                # Original data (sum of individual municipalities)
                'original_ja_sum': original_ja_sum,
                'original_nein_sum': original_nein_sum,
                'original_gueltig_sum': original_gueltig_sum,
                # Validation
                'ja_match': abs(row['ja_stimmen_absolut'] - original_ja_sum) < 0.01,
                'nein_match': abs(row['nein_stimmen_absolut'] - original_nein_sum) < 0.01,
                'gueltig_match': abs(row['gueltige_stimmen'] - original_gueltig_sum) < 0.01
            })

    validation_df = pd.DataFrame(validation_records)

    return timeline_df, original_df, aggregated_df, validation_df

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Validating merger data")
    logger.info("="*60)

    # Database path (relative to validation folder)
    db_path = Path('../data/swiss_votings.db')

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run import_all_data.py first")
        return 1

    try:
        # Connect to database
        logger.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # Find complex cases
        complex_df = find_most_complex_mergers(conn, logger)
        top_10_complex = complex_df.head(10)

        # Find simple cases
        simple_df = find_simple_cases(conn, logger)

        # Combine selection
        selected_municipalities = pd.concat([top_10_complex, simple_df], ignore_index=True)

        logger.info(f"\nSelected {len(selected_municipalities)} municipalities for validation:")
        logger.info(f"  Complex cases: {len(top_10_complex)}")
        logger.info(f"  Simple cases: {len(simple_df)}")

        # Show selection
        logger.info("\nSelected municipalities:")
        for _, row in selected_municipalities.iterrows():
            logger.info(f"  {row['current_name']} (BFS {row['current_bfs']}): {row['predecessor_count']} predecessors, depth {row['max_merge_depth']}")

        # Create validation report
        timeline_df, original_df, aggregated_df, validation_df = create_validation_report(
            conn, selected_municipalities, logger
        )

        # Save outputs to validation folder
        output_dir = Path('.')

        # 1. Summary of selected municipalities
        summary_path = output_dir / 'merger_validation_summary.csv'
        selected_municipalities.to_csv(summary_path, index=False, encoding='utf-8-sig')
        logger.info(f"\nSaved summary: {summary_path}")

        # 2. Merger timeline
        timeline_path = output_dir / 'merger_validation_timeline.csv'
        timeline_df.to_csv(timeline_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved timeline: {timeline_path}")

        # 3. Original voting data
        original_path = output_dir / 'merger_validation_original.csv'
        original_df.to_csv(original_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved original data: {original_path}")

        # 4. Aggregated voting data
        aggregated_path = output_dir / 'merger_validation_aggregated.csv'
        aggregated_df.to_csv(aggregated_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved aggregated data: {aggregated_path}")

        # 5. Validation comparison
        validation_path = output_dir / 'merger_validation_comparison.csv'
        validation_df.to_csv(validation_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved validation comparison: {validation_path}")

        # Show validation statistics
        logger.info("\n" + "="*60)
        logger.info("VALIDATION STATISTICS")
        logger.info("="*60)

        if len(validation_df) > 0:
            total_records = len(validation_df)
            ja_matches = validation_df['ja_match'].sum()
            nein_matches = validation_df['nein_match'].sum()
            gueltig_matches = validation_df['gueltig_match'].sum()

            logger.info(f"Total validation records: {total_records}")
            logger.info(f"Ja stimmen matches: {ja_matches} / {total_records} ({100*ja_matches/total_records:.1f}%)")
            logger.info(f"Nein stimmen matches: {nein_matches} / {total_records} ({100*nein_matches/total_records:.1f}%)")
            logger.info(f"Gueltige stimmen matches: {gueltig_matches} / {total_records} ({100*gueltig_matches/total_records:.1f}%)")

            # Show mismatches if any
            mismatches = validation_df[~validation_df['ja_match'] | ~validation_df['nein_match'] | ~validation_df['gueltig_match']]
            if len(mismatches) > 0:
                logger.warning(f"\nFound {len(mismatches)} mismatches - check validation_comparison.csv")
                logger.warning("Sample mismatches:")
                for _, row in mismatches.head(3).iterrows():
                    logger.warning(f"  {row['current_name']} - Voting {row['voting_date']}, Proposal {row['proposal_id']}")
                    logger.warning(f"    Aggregated: {row['aggregated_ja']:.0f} ja, {row['aggregated_nein']:.0f} nein")
                    logger.warning(f"    Original sum: {row['original_ja_sum']:.0f} ja, {row['original_nein_sum']:.0f} nein")

        # Close connection
        conn.close()

        logger.info("\n" + "="*60)
        logger.info("SUCCESS!")
        logger.info("="*60)
        logger.info("\nGenerated files:")
        logger.info(f"  {summary_path}")
        logger.info(f"  {timeline_path}")
        logger.info(f"  {original_path}")
        logger.info(f"  {aggregated_path}")
        logger.info(f"  {validation_path}")
        logger.info("\nUse these files to manually cross-reference:")
        logger.info("  - Original data shows individual municipalities before merger")
        logger.info("  - Aggregated data shows combined results after merger")
        logger.info("  - Comparison shows if sums match correctly")

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())