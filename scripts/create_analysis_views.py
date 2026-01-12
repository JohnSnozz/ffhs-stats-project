#!/usr/bin/env python3
"""
Create analysis-ready views that handle mergers and splits correctly.

Key principle: We can only aggregate historical data (mergers), never disaggregate (splits).

Strategy:
1. Use the FIRST appearance of each municipality in voting data as reference
2. For mergers (A+B→C after both appeared): aggregate to newest form
3. For splits (A→B+C after A appeared): keep A, cannot split historical data
4. Create a stable reference structure for statistical analysis

This ensures perfect data matching between historical and aggregated data.
"""

import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import sys

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'create_analysis_views_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def analyze_municipality_lifecycle(conn, logger):
    """Analyze municipality lifecycle to understand mergers and splits"""
    logger.info("Analyzing municipality lifecycle...")

    # Find first and last appearance of each municipality
    query = """
    CREATE TEMP TABLE municipality_lifecycle AS
    SELECT
        geo_id,
        geo_name,
        MIN(v.voting_date) as first_appearance,
        MAX(v.voting_date) as last_appearance,
        COUNT(DISTINCT v.voting_id) as voting_count
    FROM voting_results vr
    INNER JOIN votings v ON vr.voting_id = v.voting_id
    WHERE vr.geo_level = 'municipality'
    GROUP BY geo_id, geo_name
    """

    conn.execute(query)
    conn.commit()
    logger.info("Created municipality lifecycle temp table")

    # Get statistics
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM municipality_lifecycle")
    total = cursor.fetchone()[0]
    logger.info(f"Total municipalities in voting data: {total}")

def create_stable_municipality_mapping(conn, logger):
    """
    Create mapping that handles both mergers and splits correctly.

    Logic:
    - Follow merger chains forward (A+B→C)
    - But stop if we hit a split (cannot disaggregate)
    - Use the most aggregated form that doesn't require disaggregation
    """
    logger.info("Creating stable municipality mapping...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_stable_municipality_mapping")

    create_view_sql = """
    CREATE VIEW v_stable_municipality_mapping AS
    WITH RECURSIVE
    -- First, identify municipalities that appear in voting data
    voting_municipalities AS (
        SELECT DISTINCT
            geo_id as bfs,
            geo_name as name,
            MIN(v.voting_date) as first_appearance
        FROM voting_results vr
        INNER JOIN votings v ON vr.voting_id = v.voting_id
        WHERE vr.geo_level = 'municipality'
        GROUP BY geo_id, geo_name
    ),
    -- Identify TRUE splits: where one BFS splits into multiple on same date
    true_splits AS (
        SELECT DISTINCT old_bfs_number
        FROM municipal_changes mc1
        WHERE old_bfs_number != new_bfs_number
        AND EXISTS (
            SELECT 1
            FROM municipal_changes mc2
            WHERE mc2.old_bfs_number = mc1.old_bfs_number
            AND mc2.new_bfs_number != mc1.new_bfs_number
            AND mc2.mutation_date = mc1.mutation_date
            AND mc2.old_bfs_number != mc2.new_bfs_number
        )
    ),
    -- Track merger chains, follow mergers, stop at TRUE splits
    merger_chain AS (
        -- Base case: start with each municipality in voting data
        SELECT
            vm.bfs as original_bfs,
            vm.name as original_name,
            vm.bfs as analysis_bfs,
            vm.name as analysis_name,
            vm.first_appearance,
            0 as depth
        FROM voting_municipalities vm

        UNION

        -- Follow mergers (where multiple BFS merge into one)
        SELECT
            mc_recursive.original_bfs,
            mc_recursive.original_name,
            mch.new_bfs_number as analysis_bfs,
            mch.new_name as analysis_name,
            mc_recursive.first_appearance,
            mc_recursive.depth + 1
        FROM merger_chain mc_recursive
        INNER JOIN municipal_changes mch
            ON mc_recursive.analysis_bfs = mch.old_bfs_number
        WHERE mc_recursive.depth < 10
        -- Only follow if BFS actually changed
        AND mch.old_bfs_number != mch.new_bfs_number
        -- Only follow mergers that happened AFTER this municipality appeared in voting data
        AND mch.mutation_date >= mc_recursive.first_appearance
        -- STOP if this is a true split (one becomes many)
        AND mc_recursive.analysis_bfs NOT IN (SELECT old_bfs_number FROM true_splits)
    )
    SELECT
        original_bfs,
        original_name,
        analysis_bfs,
        analysis_name,
        first_appearance,
        MAX(depth) as merge_depth
    FROM merger_chain
    GROUP BY original_bfs
    HAVING depth = (
        SELECT MAX(depth)
        FROM merger_chain mc2
        WHERE mc2.original_bfs = merger_chain.original_bfs
    )
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Stable municipality mapping view created")

def create_analysis_ready_results_view(conn, logger):
    """
    Create view with voting results using stable municipality structure.
    This ensures perfect aggregation - no data loss, no impossible disaggregation.
    """
    logger.info("Creating analysis-ready results view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_voting_results_analysis")

    create_view_sql = """
    CREATE VIEW v_voting_results_analysis AS
    SELECT
        v.voting_id,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        p.title_fr,
        p.title_it,
        p.angenommen,
        m.analysis_bfs as municipality_id,
        m.analysis_name as municipality_name,
        -- Aggregate voting results
        SUM(vr.ja_stimmen_absolut) as ja_stimmen_absolut,
        SUM(vr.nein_stimmen_absolut) as nein_stimmen_absolut,
        SUM(vr.gueltige_stimmen) as gueltige_stimmen,
        SUM(vr.eingelegte_stimmzettel) as eingelegte_stimmzettel,
        SUM(vr.anzahl_stimmberechtigte) as anzahl_stimmberechtigte,
        -- Recalculate percentages
        CASE
            WHEN SUM(vr.gueltige_stimmen) > 0
            THEN ROUND(100.0 * SUM(vr.ja_stimmen_absolut) / SUM(vr.gueltige_stimmen), 2)
            ELSE NULL
        END as ja_prozent,
        CASE
            WHEN SUM(vr.anzahl_stimmberechtigte) > 0
            THEN ROUND(100.0 * SUM(vr.eingelegte_stimmzettel) / SUM(vr.anzahl_stimmberechtigte), 2)
            ELSE NULL
        END as stimmbeteiligung,
        -- Metadata
        COUNT(DISTINCT vr.geo_id) as source_municipality_count,
        GROUP_CONCAT(DISTINCT vr.geo_id) as source_bfs_numbers
    FROM votings v
    INNER JOIN proposals p ON v.voting_id = p.voting_id
    INNER JOIN voting_results vr ON p.proposal_id = vr.proposal_id
    INNER JOIN v_stable_municipality_mapping m ON vr.geo_id = m.original_bfs
    WHERE vr.geo_level = 'municipality'
    GROUP BY
        v.voting_id,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        p.title_fr,
        p.title_it,
        p.angenommen,
        m.analysis_bfs,
        m.analysis_name
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Analysis-ready results view created")

def create_data_quality_view(conn, logger):
    """
    Create view to show data quality and aggregation details.
    """
    logger.info("Creating data quality view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_municipality_data_quality")

    create_view_sql = """
    CREATE VIEW v_municipality_data_quality AS
    SELECT
        m.analysis_bfs,
        m.analysis_name,
        COUNT(DISTINCT m.original_bfs) as source_count,
        GROUP_CONCAT(DISTINCT m.original_bfs || ' (' || m.original_name || ')') as sources,
        MIN(m.first_appearance) as first_appearance,
        MAX(m.merge_depth) as merge_depth,
        COUNT(DISTINCT vr.voting_id) as voting_participation_count
    FROM v_stable_municipality_mapping m
    LEFT JOIN voting_results vr
        ON m.original_bfs = vr.geo_id
        AND vr.geo_level = 'municipality'
    GROUP BY m.analysis_bfs, m.analysis_name
    ORDER BY source_count DESC, m.analysis_name
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Data quality view created")

def verify_perfect_matching(conn, logger):
    """
    Verify that aggregated data perfectly matches sum of original data.
    """
    logger.info("Verifying perfect data matching...")

    cursor = conn.cursor()

    # Test: Compare sum of original data vs aggregated data for a sample voting
    query = """
    WITH original_sums AS (
        SELECT
            p.proposal_id,
            SUM(vr.ja_stimmen_absolut) as original_ja,
            SUM(vr.nein_stimmen_absolut) as original_nein
        FROM voting_results vr
        INNER JOIN proposals p ON vr.proposal_id = p.proposal_id
        WHERE vr.geo_level = 'municipality'
        GROUP BY p.proposal_id
    ),
    aggregated_sums AS (
        SELECT
            proposal_id,
            SUM(ja_stimmen_absolut) as aggregated_ja,
            SUM(nein_stimmen_absolut) as aggregated_nein
        FROM v_voting_results_analysis
        GROUP BY proposal_id
    )
    SELECT
        o.proposal_id,
        o.original_ja,
        a.aggregated_ja,
        o.original_nein,
        a.aggregated_nein,
        ABS(o.original_ja - a.aggregated_ja) as ja_diff,
        ABS(o.original_nein - a.aggregated_nein) as nein_diff
    FROM original_sums o
    INNER JOIN aggregated_sums a ON o.proposal_id = a.proposal_id
    WHERE ABS(o.original_ja - a.aggregated_ja) > 0.1
       OR ABS(o.original_nein - a.aggregated_nein) > 0.1
    ORDER BY ja_diff DESC
    LIMIT 10
    """

    cursor.execute(query)
    mismatches = cursor.fetchall()

    if len(mismatches) == 0:
        logger.info("✓ PERFECT MATCH! All aggregated data matches original data exactly.")
    else:
        logger.warning(f"Found {len(mismatches)} proposals with mismatches:")
        for row in mismatches:
            logger.warning(f"  Proposal {row[0]}: Ja diff={row[5]}, Nein diff={row[6]}")

    # Get statistics
    cursor.execute("""
        SELECT
            COUNT(*) as total_municipalities,
            COUNT(DISTINCT analysis_bfs) as analysis_municipalities,
            SUM(CASE WHEN source_count > 1 THEN 1 ELSE 0 END) as aggregated_count,
            MAX(source_count) as max_aggregation
        FROM v_municipality_data_quality
    """)

    stats = cursor.fetchone()
    logger.info(f"\nMunicipality statistics:")
    logger.info(f"  Original municipalities in voting data: {stats[0]}")
    logger.info(f"  Analysis municipalities (after aggregation): {stats[1]}")
    logger.info(f"  Municipalities that aggregated others: {stats[2]}")
    logger.info(f"  Maximum aggregation: {stats[3]} municipalities into 1")

def create_indexes(conn, logger):
    """Create indexes for performance"""
    logger.info("Creating indexes...")

    cursor = conn.cursor()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_vr_geo_voting ON voting_results(geo_level, geo_id, voting_id)",
        "CREATE INDEX IF NOT EXISTS idx_vr_proposal_geo ON voting_results(proposal_id, geo_level, geo_id)"
    ]

    for idx_sql in indexes:
        try:
            cursor.execute(idx_sql)
        except Exception as e:
            logger.warning(f"Could not create index: {e}")

    conn.commit()
    logger.info("Indexes created")

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Creating analysis-ready views with perfect data matching")
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

        # Analyze lifecycle
        analyze_municipality_lifecycle(conn, logger)

        # Create views
        create_stable_municipality_mapping(conn, logger)
        create_analysis_ready_results_view(conn, logger)
        create_data_quality_view(conn, logger)

        # Create indexes
        create_indexes(conn, logger)

        # Verify
        verify_perfect_matching(conn, logger)

        # Show examples
        logger.info("\n" + "="*60)
        logger.info("EXAMPLES")
        logger.info("="*60)

        cursor = conn.cursor()

        # Show aggregated municipalities
        cursor.execute("""
            SELECT analysis_bfs, analysis_name, source_count, sources
            FROM v_municipality_data_quality
            WHERE source_count > 1
            ORDER BY source_count DESC
            LIMIT 5
        """)

        logger.info("\nTop municipalities with aggregation:")
        for row in cursor.fetchall():
            logger.info(f"  {row[1]} (BFS {row[0]}): {row[2]} sources")
            logger.info(f"    Sources: {row[3][:100]}...")

        # Close connection
        conn.close()

        logger.info("\n" + "="*60)
        logger.info("SUCCESS!")
        logger.info("="*60)
        logger.info("\nViews created:")
        logger.info("  v_stable_municipality_mapping - Municipality mapping for analysis")
        logger.info("  v_voting_results_analysis - Ready for statistical analysis")
        logger.info("  v_municipality_data_quality - Data quality information")
        logger.info("\nUse v_voting_results_analysis for all statistical analysis!")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())