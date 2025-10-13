#!/usr/bin/env python3
"""
Test the merger views to understand how municipal mergers affect the data.
"""

import sqlite3
import pandas as pd
from pathlib import Path

def main():
    # Connect to database (relative to validation folder)
    db_path = Path('../data/swiss_votings.db')
    conn = sqlite3.connect(db_path)

    print("="*60)
    print("TESTING MUNICIPAL MERGER VIEWS")
    print("="*60)

    # 1. Check municipalities that appear in different forms over time
    print("\n1. Municipalities that appear in voting data and later merged:")
    query = """
    SELECT DISTINCT
        vr.geo_id as old_bfs,
        vr.geo_name as old_name,
        mc.new_bfs_number as new_bfs,
        mc.new_name as new_name,
        mc.mutation_date,
        MIN(v.voting_date) as last_appearance_old,
        MAX(v.voting_date) as first_appearance_new
    FROM voting_results vr
    INNER JOIN municipal_changes mc ON vr.geo_id = mc.old_bfs_number
    INNER JOIN votings v ON vr.voting_id = v.voting_id
    WHERE vr.geo_level = 'municipality'
    AND mc.old_bfs_number != mc.new_bfs_number
    GROUP BY vr.geo_id, mc.new_bfs_number
    ORDER BY mc.mutation_date
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

    # 2. Check if successor municipalities appear in the data
    print("\n2. Checking if successor municipalities appear after merger:")
    query = """
    WITH merger_transitions AS (
        SELECT DISTINCT
            mc.old_bfs_number,
            mc.old_name,
            mc.new_bfs_number,
            mc.new_name,
            mc.mutation_date
        FROM municipal_changes mc
        WHERE mc.old_bfs_number != mc.new_bfs_number
        AND mc.old_bfs_number IN (
            SELECT DISTINCT geo_id
            FROM voting_results
            WHERE geo_level = 'municipality'
        )
    )
    SELECT
        mt.*,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM voting_results vr2
                WHERE vr2.geo_id = mt.new_bfs_number
                AND vr2.geo_level = 'municipality'
            ) THEN 'YES'
            ELSE 'NO'
        END as successor_in_data
    FROM merger_transitions mt
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

    # 3. Find municipalities that actually transitioned during our data period
    print("\n3. Municipalities that actually transitioned during data period:")
    query = """
    WITH transitions AS (
        SELECT
            mc.old_bfs_number,
            mc.old_name,
            mc.new_bfs_number,
            mc.new_name,
            mc.mutation_date,
            (SELECT MAX(voting_date) FROM votings v
             JOIN voting_results vr ON v.voting_id = vr.voting_id
             WHERE vr.geo_id = mc.old_bfs_number
             AND vr.geo_level = 'municipality') as last_old_voting,
            (SELECT MIN(voting_date) FROM votings v
             JOIN voting_results vr ON v.voting_id = vr.voting_id
             WHERE vr.geo_id = mc.new_bfs_number
             AND vr.geo_level = 'municipality') as first_new_voting
        FROM municipal_changes mc
        WHERE mc.old_bfs_number != mc.new_bfs_number
    )
    SELECT *
    FROM transitions
    WHERE last_old_voting IS NOT NULL
    AND first_new_voting IS NOT NULL
    ORDER BY mutation_date
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

    # 4. Check the actual effect of using the view
    print("\n4. Example: Effect of merger on a specific voting (latest):")
    query = """
    SELECT
        'Original' as data_type,
        COUNT(DISTINCT geo_id) as municipality_count,
        SUM(ja_stimmen_absolut) as total_yes,
        SUM(nein_stimmen_absolut) as total_no
    FROM voting_results vr
    JOIN votings v ON vr.voting_id = v.voting_id
    WHERE vr.geo_level = 'municipality'
    AND v.voting_date = (SELECT MAX(voting_date) FROM votings)

    UNION ALL

    SELECT
        'After Mergers' as data_type,
        COUNT(DISTINCT municipality_id) as municipality_count,
        SUM(ja_stimmen_absolut) as total_yes,
        SUM(nein_stimmen_absolut) as total_no
    FROM v_voting_results_current
    WHERE voting_date = (SELECT MAX(voting_date) FROM votings)
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

    # 5. Find municipalities with multiple predecessors (true mergers)
    print("\n5. Current municipalities formed from multiple predecessors:")
    query = """
    SELECT
        new_bfs_number as current_bfs,
        new_name as current_name,
        COUNT(*) as predecessor_count,
        GROUP_CONCAT(old_bfs_number || ' (' || old_name || ')') as predecessors,
        mutation_date
    FROM municipal_changes
    WHERE old_bfs_number != new_bfs_number
    AND is_merger = 1
    GROUP BY new_bfs_number, mutation_date
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC, mutation_date DESC
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

    # 6. Test the main view with a real example
    print("\n6. Testing v_voting_results_current with merged municipalities:")
    query = """
    SELECT
        municipality_id,
        municipality_name,
        merged_municipality_count,
        original_bfs_numbers,
        ja_stimmen_absolut,
        nein_stimmen_absolut
    FROM v_voting_results_current
    WHERE merged_municipality_count > 1
    AND voting_date = (SELECT MAX(voting_date) FROM votings)
    LIMIT 5
    """
    df = pd.read_sql_query(query, conn)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("No merged municipalities found in the latest voting")

    # Close connection
    conn.close()

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("""
The view system handles municipal mergers by:
1. Tracking all historical BFS numbers that appear in voting data
2. Following the chain of mergers to find current successors
3. Aggregating voting results from merged municipalities
4. Providing a consistent current municipal structure for analysis

Note: Many mergers occurred before our data period (2000-2025) or
involve municipalities that may not participate in federal votes,
so the actual number of observable mergers in the data may be limited.
    """)

if __name__ == "__main__":
    main()