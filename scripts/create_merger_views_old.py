#!/usr/bin/env python3
"""
Create database views that handle municipal mergers and provide voting results
mapped to the current municipal structure.

The views created:
1. v_municipality_mapping - Maps all BFS numbers to their final successors
2. v_voting_results_current - Voting results aggregated by current municipalities
3. v_municipality_evolution - Timeline of how municipalities changed
4. v_merger_statistics - Statistics about mergers in the dataset

Usage:
    python create_merger_views.py

After running, use v_voting_results_current for all analysis that requires
current municipal boundaries, regardless of when the voting took place.
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
    log_file = log_dir / f'create_views_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def create_municipality_mapping_view(conn, logger):
    """
    Create a view that maps all BFS numbers (historical and current) to their
    final successor municipality. This handles cascading mergers where a
    municipality may merge multiple times.

    Example: If A+B→C in 2010, then C+D→E in 2015, both A and B will map to E.
    """
    logger.info("Creating municipality mapping view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_municipality_mapping")

    create_view_sql = """
    CREATE VIEW v_municipality_mapping AS
    WITH RECURSIVE successor_chain AS (
        -- Start with all unique BFS numbers that appear in voting results
        SELECT DISTINCT
            geo_id as original_bfs,
            geo_name as original_name,
            geo_id as current_bfs,
            geo_name as current_name,
            0 as merge_depth
        FROM voting_results
        WHERE geo_level = 'municipality'

        UNION

        -- Follow the chain of mergers (only where BFS number actually changed)
        SELECT
            sc.original_bfs,
            sc.original_name,
            mc.new_bfs_number as current_bfs,
            mc.new_name as current_name,
            sc.merge_depth + 1
        FROM successor_chain sc
        INNER JOIN municipal_changes mc ON sc.current_bfs = mc.old_bfs_number
        WHERE sc.merge_depth < 10  -- Reasonable safety limit
        AND mc.old_bfs_number != mc.new_bfs_number  -- Only actual changes, not renames
    )
    SELECT
        original_bfs,
        original_name,
        current_bfs,
        current_name,
        MAX(merge_depth) as total_mergers
    FROM successor_chain
    GROUP BY original_bfs
    HAVING merge_depth = (
        SELECT MAX(merge_depth)
        FROM successor_chain sc2
        WHERE sc2.original_bfs = successor_chain.original_bfs
    )
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Municipality mapping view created successfully")

def create_voting_results_current_view(conn, logger):
    """
    Create the main view that shows all voting results mapped to current municipalities.
    This aggregates voting data from merged municipalities, so you get consistent
    results regardless of when municipalities merged.

    Key features:
    - Aggregates ja_stimmen, nein_stimmen, etc. from all predecessor municipalities
    - Recalculates percentages based on aggregated totals
    - Tracks how many original municipalities contributed to each current one
    """
    logger.info("Creating current voting results view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_voting_results_current")

    create_view_sql = """
    CREATE VIEW v_voting_results_current AS
    SELECT
        v.voting_id,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        p.title_fr,
        p.title_it,
        p.angenommen,
        m.current_bfs as municipality_id,
        m.current_name as municipality_name,
        -- Aggregate voting results from all predecessor municipalities
        SUM(vr.ja_stimmen_absolut) as ja_stimmen_absolut,
        SUM(vr.nein_stimmen_absolut) as nein_stimmen_absolut,
        SUM(vr.gueltige_stimmen) as gueltige_stimmen,
        SUM(vr.eingelegte_stimmzettel) as eingelegte_stimmzettel,
        SUM(vr.anzahl_stimmberechtigte) as anzahl_stimmberechtigte,
        -- Recalculate percentages based on aggregated data
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
        -- Metadata about the aggregation
        COUNT(DISTINCT vr.geo_id) as merged_municipality_count,
        GROUP_CONCAT(DISTINCT vr.geo_id) as original_bfs_numbers
    FROM votings v
    INNER JOIN proposals p ON v.voting_id = p.voting_id
    INNER JOIN voting_results vr ON p.proposal_id = vr.proposal_id
    INNER JOIN v_municipality_mapping m ON vr.geo_id = m.original_bfs
    WHERE vr.geo_level = 'municipality'
    GROUP BY
        v.voting_id,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        p.title_fr,
        p.title_it,
        p.angenommen,
        m.current_bfs,
        m.current_name
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Current voting results view created successfully")

def create_municipality_evolution_view(conn, logger):
    """
    Create a view showing the timeline of municipal changes.
    Useful for understanding when and how municipalities evolved.
    """
    logger.info("Creating municipality evolution view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_municipality_evolution")

    create_view_sql = """
    CREATE VIEW v_municipality_evolution AS
    SELECT
        mc.old_bfs_number,
        mc.old_name,
        mc.old_canton,
        mc.new_bfs_number,
        mc.new_name,
        mc.new_canton,
        mc.mutation_date,
        mc.mutation_type,
        CASE
            WHEN mc.is_merger = 1 THEN 'Merger'
            WHEN mc.is_split = 1 THEN 'Split'
            WHEN mc.is_rename = 1 THEN 'Rename'
            WHEN mc.is_reassignment = 1 THEN 'Reassignment'
            ELSE 'Other'
        END as change_category,
        -- Count how many municipalities merged into this one on this date
        (SELECT COUNT(*)
         FROM municipal_changes mc2
         WHERE mc2.new_bfs_number = mc.new_bfs_number
         AND mc2.mutation_date = mc.mutation_date
         AND mc2.old_bfs_number != mc2.new_bfs_number) as municipalities_in_merger,
        -- Check if this municipality appeared in voting data before merger
        EXISTS (
            SELECT 1 FROM voting_results vr
            WHERE vr.geo_id = mc.old_bfs_number
            AND vr.geo_level = 'municipality'
        ) as was_in_voting_data,
        -- Check if this municipality was later merged again
        EXISTS (
            SELECT 1 FROM municipal_changes mc3
            WHERE mc3.old_bfs_number = mc.new_bfs_number
            AND mc3.mutation_date > mc.mutation_date
            AND mc3.old_bfs_number != mc3.new_bfs_number
        ) as merged_again_later
    FROM municipal_changes mc
    WHERE mc.old_bfs_number != mc.new_bfs_number  -- Only actual changes
    ORDER BY mc.mutation_date, mc.new_bfs_number
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Municipality evolution view created successfully")

def create_merger_statistics_view(conn, logger):
    """
    Create a view with statistics about mergers in the dataset.
    Helps understand the impact of mergers on data analysis.
    """
    logger.info("Creating merger statistics view...")

    cursor = conn.cursor()

    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_merger_statistics")

    create_view_sql = """
    CREATE VIEW v_merger_statistics AS
    SELECT
        v.voting_id,
        v.voting_date,
        p.proposal_id,
        p.title_de,
        -- Original municipality count in this voting
        COUNT(DISTINCT CASE
            WHEN vr.geo_level = 'municipality' THEN vr.geo_id
        END) as original_municipality_count,
        -- Current municipality count after applying mergers
        COUNT(DISTINCT CASE
            WHEN vr.geo_level = 'municipality' THEN m.current_bfs
        END) as current_municipality_count,
        -- How many were reduced by mergers
        COUNT(DISTINCT CASE
            WHEN vr.geo_level = 'municipality' THEN vr.geo_id
        END) - COUNT(DISTINCT CASE
            WHEN vr.geo_level = 'municipality' THEN m.current_bfs
        END) as municipalities_merged_count,
        -- Total votes (should remain the same)
        SUM(CASE
            WHEN vr.geo_level = 'municipality' THEN vr.ja_stimmen_absolut
            ELSE 0
        END) as total_yes_votes,
        SUM(CASE
            WHEN vr.geo_level = 'municipality' THEN vr.nein_stimmen_absolut
            ELSE 0
        END) as total_no_votes
    FROM votings v
    INNER JOIN proposals p ON v.voting_id = p.voting_id
    INNER JOIN voting_results vr ON p.proposal_id = vr.proposal_id
    LEFT JOIN v_municipality_mapping m ON vr.geo_id = m.original_bfs
    GROUP BY v.voting_id, v.voting_date, p.proposal_id, p.title_de
    """

    cursor.execute(create_view_sql)
    conn.commit()
    logger.info("Merger statistics view created successfully")

def create_indexes_for_views(conn, logger):
    """Create indexes to improve view performance"""
    logger.info("Creating indexes for view performance...")

    cursor = conn.cursor()

    # Additional indexes for better view performance
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_mc_old_new ON municipal_changes(old_bfs_number, new_bfs_number)",
        "CREATE INDEX IF NOT EXISTS idx_mc_date ON municipal_changes(mutation_date)",
        "CREATE INDEX IF NOT EXISTS idx_vr_geo ON voting_results(geo_level, geo_id)",
        "CREATE INDEX IF NOT EXISTS idx_vr_proposal ON voting_results(proposal_id)",
        "CREATE INDEX IF NOT EXISTS idx_vr_voting ON voting_results(voting_id)"
    ]

    for idx_sql in indexes:
        try:
            cursor.execute(idx_sql)
            logger.debug(f"Created index: {idx_sql[:50]}...")
        except Exception as e:
            logger.warning(f"Could not create index: {e}")

    conn.commit()
    logger.info("Indexes created successfully")

def verify_views(conn, logger):
    """Verify the views and show summary statistics"""
    logger.info("Verifying views and gathering statistics...")

    cursor = conn.cursor()

    # Check municipality mapping
    cursor.execute("""
        SELECT
            COUNT(*) as total_municipalities,
            COUNT(DISTINCT original_bfs) as unique_historical,
            COUNT(DISTINCT current_bfs) as unique_current,
            SUM(CASE WHEN total_mergers > 0 THEN 1 ELSE 0 END) as merged_count,
            MAX(total_mergers) as max_merger_depth
        FROM v_municipality_mapping
    """)
    result = cursor.fetchone()
    logger.info(f"Municipality mappings:")
    logger.info(f"  Total mappings: {result[0]}")
    logger.info(f"  Historical municipalities: {result[1]}")
    logger.info(f"  Current municipalities: {result[2]}")
    logger.info(f"  Municipalities that merged: {result[3]}")
    logger.info(f"  Maximum cascade depth: {result[4]}")

    # Check voting results view
    cursor.execute("""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT voting_id) as unique_votings,
            COUNT(DISTINCT proposal_id) as unique_proposals,
            COUNT(DISTINCT municipality_id) as unique_municipalities,
            MAX(merged_municipality_count) as max_merged,
            SUM(CASE WHEN merged_municipality_count > 1 THEN 1 ELSE 0 END) as records_with_mergers
        FROM v_voting_results_current
    """)
    result = cursor.fetchone()
    logger.info(f"\nCurrent voting results view:")
    logger.info(f"  Total records: {result[0]}")
    logger.info(f"  Votings: {result[1]}")
    logger.info(f"  Proposals: {result[2]}")
    logger.info(f"  Current municipalities: {result[3]}")
    logger.info(f"  Max municipalities merged together: {result[4]}")
    logger.info(f"  Records showing mergers: {result[5]}")

    # Show examples of merged municipalities
    cursor.execute("""
        SELECT DISTINCT
            municipality_id,
            municipality_name,
            merged_municipality_count,
            original_bfs_numbers
        FROM v_voting_results_current
        WHERE merged_municipality_count > 1
        ORDER BY merged_municipality_count DESC
        LIMIT 3
    """)

    logger.info("\nExamples of merged municipalities:")
    for row in cursor.fetchall():
        logger.info(f"  {row[1]} (BFS {row[0]}): {row[2]} municipalities merged")
        logger.info(f"    Original BFS: {row[3]}")

    # Show merger statistics for a recent voting
    cursor.execute("""
        SELECT
            voting_date,
            original_municipality_count,
            current_municipality_count,
            municipalities_merged_count
        FROM v_merger_statistics
        WHERE voting_date = (SELECT MAX(voting_date) FROM votings)
        LIMIT 1
    """)
    result = cursor.fetchone()
    if result:
        logger.info(f"\nMerger effect on most recent voting ({result[0]}):")
        logger.info(f"  Original municipalities: {result[1]}")
        logger.info(f"  After mergers: {result[2]}")
        logger.info(f"  Reduction: {result[3]}")

def print_usage_examples(logger):
    """Print example queries for using the views"""
    logger.info("\n" + "="*60)
    logger.info("USAGE EXAMPLES")
    logger.info("="*60)

    examples = [
        ("Get voting results for latest vote with current municipalities:", """
SELECT
    municipality_id,
    municipality_name,
    ja_stimmen_absolut,
    nein_stimmen_absolut,
    ja_prozent,
    stimmbeteiligung,
    merged_municipality_count
FROM v_voting_results_current
WHERE voting_date = (SELECT MAX(voting_date) FROM votings)
ORDER BY municipality_name;"""),

        ("Find which municipalities merged into a specific one:", """
SELECT
    original_bfs,
    original_name
FROM v_municipality_mapping
WHERE current_bfs = '5226'  -- Capriasca
AND total_mergers > 0;"""),

        ("See timeline of mergers affecting the data:", """
SELECT
    mutation_date,
    old_bfs_number,
    old_name,
    new_bfs_number,
    new_name,
    municipalities_in_merger
FROM v_municipality_evolution
WHERE was_in_voting_data = 1
ORDER BY mutation_date DESC;"""),

        ("Compare original vs merged municipality counts per voting:", """
SELECT
    voting_date,
    title_de,
    original_municipality_count,
    current_municipality_count,
    municipalities_merged_count
FROM v_merger_statistics
WHERE municipalities_merged_count > 0
ORDER BY municipalities_merged_count DESC
LIMIT 10;"""),

        ("Analyze voting patterns for a municipality over time (with mergers handled):", """
SELECT
    voting_date,
    title_de,
    ja_prozent,
    stimmbeteiligung,
    merged_municipality_count
FROM v_voting_results_current
WHERE municipality_id = '5226'  -- Capriasca
ORDER BY voting_date;""")
    ]

    for description, query in examples:
        logger.info(f"\n{description}")
        logger.info(query)

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Creating database views for municipal merger handling")
    logger.info("="*60)

    # Database path
    db_path = Path('data/swiss_votings.db')

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run import_all_data.py first to create the database")
        return 1

    try:
        # Connect to database
        logger.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # Create all views
        create_municipality_mapping_view(conn, logger)
        create_voting_results_current_view(conn, logger)
        create_municipality_evolution_view(conn, logger)
        create_merger_statistics_view(conn, logger)

        # Create indexes
        create_indexes_for_views(conn, logger)

        # Verify views
        verify_views(conn, logger)

        # Print usage examples
        print_usage_examples(logger)

        # Close connection
        conn.close()
        logger.info("\nDatabase connection closed")

        logger.info("\n" + "="*60)
        logger.info("SUCCESS!")
        logger.info("="*60)
        logger.info("Views created successfully!")
        logger.info("\nMain view: v_voting_results_current")
        logger.info("  Use this for all analysis with current municipal boundaries")
        logger.info("\nSupporting views:")
        logger.info("  v_municipality_mapping - BFS number mappings")
        logger.info("  v_municipality_evolution - Timeline of changes")
        logger.info("  v_merger_statistics - Impact statistics per voting")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())