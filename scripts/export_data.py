#!/usr/bin/env python3
"""
Export voting data to CSV with current municipal structure.

Creates a CSV file with:
- First column: Geographic entity (municipality, district, canton) with current names
- For each proposal: 3 columns (ja_absolut, nein_absolut, ja_prozent)
- Column headers use proposal_id (e.g., 194_ja, 194_nein, 194_pct)

Output: data/voting_results_export.csv
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import sys

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'export_csv_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def get_all_proposals(conn, logger):
    """Get all proposals ordered by voting date and proposal id"""
    logger.info("Fetching all proposals...")

    query = """
    SELECT
        p.proposal_id,
        v.voting_date,
        p.title_de
    FROM proposals p
    INNER JOIN votings v ON p.voting_id = v.voting_id
    ORDER BY v.voting_date, p.proposal_id
    """

    df = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(df)} proposals across {df['voting_date'].nunique()} voting dates")

    return df

def export_municipalities(conn, logger):
    """Export municipalities with current structure"""
    logger.info("Exporting municipalities with current structure...")

    # Get all proposals
    proposals_df = get_all_proposals(conn, logger)

    # Get unique analysis municipalities (corrected for mergers/splits)
    query = """
    SELECT DISTINCT
        municipality_id,
        municipality_name
    FROM v_voting_results_analysis
    ORDER BY municipality_name
    """

    municipalities = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(municipalities)} current municipalities")

    # Create base dataframe
    result_df = pd.DataFrame({
        'geo_type': 'municipality',
        'geo_id': municipalities['municipality_id'],
        'geo_name': municipalities['municipality_name']
    })

    # For each proposal, add 3 columns
    for _, proposal in proposals_df.iterrows():
        proposal_id = proposal['proposal_id']

        # Get data for this proposal
        query = f"""
        SELECT
            municipality_id,
            ja_stimmen_absolut,
            nein_stimmen_absolut,
            ja_prozent
        FROM v_voting_results_analysis
        WHERE proposal_id = {proposal_id}
        """

        proposal_data = pd.read_sql_query(query, conn)

        # Merge with result dataframe
        result_df = result_df.merge(
            proposal_data,
            left_on='geo_id',
            right_on='municipality_id',
            how='left'
        )

        # Rename columns
        result_df = result_df.rename(columns={
            'ja_stimmen_absolut': f'{proposal_id}_ja',
            'nein_stimmen_absolut': f'{proposal_id}_nein',
            'ja_prozent': f'{proposal_id}_pct'
        })

        # Drop the extra municipality_id column
        if 'municipality_id' in result_df.columns:
            result_df = result_df.drop('municipality_id', axis=1)

    logger.info(f"Created municipality export with {len(result_df)} rows and {len(result_df.columns)} columns")
    return result_df

def export_districts(conn, logger):
    """Export districts"""
    logger.info("Exporting districts...")

    # Get all proposals
    proposals_df = get_all_proposals(conn, logger)

    # Get unique districts
    query = """
    SELECT DISTINCT
        geo_id,
        geo_name
    FROM voting_results
    WHERE geo_level = 'district'
    ORDER BY geo_name
    """

    districts = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(districts)} districts")

    # Create base dataframe
    result_df = pd.DataFrame({
        'geo_type': 'district',
        'geo_id': districts['geo_id'],
        'geo_name': districts['geo_name']
    })

    # For each proposal, add 3 columns
    for _, proposal in proposals_df.iterrows():
        proposal_id = proposal['proposal_id']

        # Get data for this proposal
        query = f"""
        SELECT
            geo_id,
            ja_stimmen_absolut,
            nein_stimmen_absolut,
            ROUND(100.0 * ja_stimmen_absolut / NULLIF(gueltige_stimmen, 0), 2) as ja_prozent
        FROM voting_results
        WHERE proposal_id = {proposal_id}
        AND geo_level = 'district'
        """

        proposal_data = pd.read_sql_query(query, conn)

        # Merge with result dataframe
        result_df = result_df.merge(
            proposal_data,
            on='geo_id',
            how='left'
        )

        # Rename columns
        result_df = result_df.rename(columns={
            'ja_stimmen_absolut': f'{proposal_id}_ja',
            'nein_stimmen_absolut': f'{proposal_id}_nein',
            'ja_prozent': f'{proposal_id}_pct'
        })

    logger.info(f"Created district export with {len(result_df)} rows and {len(result_df.columns)} columns")
    return result_df

def export_cantons(conn, logger):
    """Export cantons"""
    logger.info("Exporting cantons...")

    # Get all proposals
    proposals_df = get_all_proposals(conn, logger)

    # Get unique cantons
    query = """
    SELECT DISTINCT
        geo_id,
        geo_name
    FROM voting_results
    WHERE geo_level = 'canton'
    ORDER BY geo_name
    """

    cantons = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(cantons)} cantons")

    # Create base dataframe
    result_df = pd.DataFrame({
        'geo_type': 'canton',
        'geo_id': cantons['geo_id'],
        'geo_name': cantons['geo_name']
    })

    # For each proposal, add 3 columns
    for _, proposal in proposals_df.iterrows():
        proposal_id = proposal['proposal_id']

        # Get data for this proposal
        query = f"""
        SELECT
            geo_id,
            ja_stimmen_absolut,
            nein_stimmen_absolut,
            ROUND(100.0 * ja_stimmen_absolut / NULLIF(gueltige_stimmen, 0), 2) as ja_prozent
        FROM voting_results
        WHERE proposal_id = {proposal_id}
        AND geo_level = 'canton'
        """

        proposal_data = pd.read_sql_query(query, conn)

        # Merge with result dataframe
        result_df = result_df.merge(
            proposal_data,
            on='geo_id',
            how='left'
        )

        # Rename columns
        result_df = result_df.rename(columns={
            'ja_stimmen_absolut': f'{proposal_id}_ja',
            'nein_stimmen_absolut': f'{proposal_id}_nein',
            'ja_prozent': f'{proposal_id}_pct'
        })

    logger.info(f"Created canton export with {len(result_df)} rows and {len(result_df.columns)} columns")
    return result_df

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Exporting voting data to CSV")
    logger.info("="*60)

    # Database path
    db_path = Path('data/swiss_votings.db')

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run import_all_data.py first")
        return 1

    try:
        # Connect to database
        logger.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # Export each level
        municipalities_df = export_municipalities(conn, logger)
        districts_df = export_districts(conn, logger)
        cantons_df = export_cantons(conn, logger)

        # Combine all levels
        logger.info("Combining all geographic levels...")
        combined_df = pd.concat([
            municipalities_df,
            districts_df,
            cantons_df
        ], ignore_index=True)

        # Save to CSV
        output_path = Path('data/voting_results_export.csv')
        logger.info(f"Saving to {output_path}...")
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')

        logger.info(f"\nExport complete!")
        logger.info(f"  Total rows: {len(combined_df)}")
        logger.info(f"  Total columns: {len(combined_df.columns)}")
        logger.info(f"  Municipalities: {len(municipalities_df)}")
        logger.info(f"  Districts: {len(districts_df)}")
        logger.info(f"  Cantons: {len(cantons_df)}")
        logger.info(f"  Output file: {output_path}")

        # Close connection
        conn.close()

        logger.info("="*60)
        logger.info("SUCCESS!")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())