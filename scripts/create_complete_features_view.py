#!/usr/bin/env python3
"""
Create a complete features view that includes:
1. Original continuous features
2. ESTV income data
3. Aggregated data for fusion municipalities from predecessors

This ensures 100% coverage for all municipalities in voting data.
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / 'data' / 'processed' / 'swiss_votings.db'


def create_complete_view():
    """Create a complete features table with all municipalities."""
    conn = sqlite3.connect(DB_PATH)

    # 1. Get all unique municipalities from voting data (excluding aggregations and expats)
    voting_munis = pd.read_sql_query("""
        SELECT DISTINCT CAST(geo_id AS INTEGER) as bfs_nr, geo_name
        FROM voting_results
        WHERE CAST(geo_id AS INTEGER) > 100
        AND CAST(geo_id AS INTEGER) < 9000
        AND geo_name NOT LIKE 'Bezirk%'
        AND geo_name NOT LIKE 'District%'
        AND geo_name NOT LIKE 'Distretto%'
        AND geo_name NOT LIKE 'Wahlkreis%'
        AND geo_name NOT LIKE 'Region%'
        AND geo_name NOT LIKE 'Kanton%'
        AND geo_name NOT LIKE 'Canton%'
    """, conn)
    logger.info(f"Found {len(voting_munis)} municipalities in voting data")

    # 2. Get continuous features
    cont_features = pd.read_sql_query("""
        SELECT * FROM municipality_continuous_features
    """, conn)
    logger.info(f"Loaded {len(cont_features)} municipalities with continuous features")

    # 3. Get ESTV income data
    estv_income = pd.read_sql_query("""
        SELECT bfs_nr, steuerbares_einkommen_pro_kopf,
               pct_einkommen_ueber_100k, pct_einkommen_unter_40k
        FROM estv_income_2020
    """, conn)
    logger.info(f"Loaded {len(estv_income)} municipalities with ESTV income data")

    # 4. Find missing municipalities and their predecessors
    missing_bfs = set(voting_munis['bfs_nr']) - set(cont_features['bfs_nr'])
    logger.info(f"Missing municipalities: {len(missing_bfs)}")

    # Get merger information - only predecessors that exist in our features
    mergers = pd.read_sql_query("""
        SELECT CAST(mc.new_bfs_number AS INTEGER) as new_bfs_number,
               CAST(mc.old_bfs_number AS INTEGER) as old_bfs_number,
               mc.old_name
        FROM municipal_changes mc
        INNER JOIN municipality_continuous_features mcf ON mc.old_bfs_number = mcf.bfs_nr
        WHERE mc.is_merger = 1
    """, conn)
    logger.info(f"Found {len(mergers)} merger records with available predecessor data")

    # 5. Create aggregated data for fusion municipalities
    aggregated_rows = []
    for new_bfs in missing_bfs:
        # Find predecessors from the mergers dataframe
        pred_rows = mergers[mergers['new_bfs_number'] == new_bfs]
        predecessors = pred_rows['old_bfs_number'].tolist()

        if not predecessors:
            # Try direct query
            pred_direct = pd.read_sql_query(f"""
                SELECT CAST(mc.old_bfs_number AS INTEGER) as old_bfs_number
                FROM municipal_changes mc
                INNER JOIN municipality_continuous_features mcf ON mc.old_bfs_number = mcf.bfs_nr
                WHERE mc.is_merger = 1 AND mc.new_bfs_number = {new_bfs}
            """, conn)
            predecessors = pred_direct['old_bfs_number'].tolist()

        if not predecessors:
            logger.warning(f"No predecessors found for BFS {new_bfs}")
            continue

        logger.info(f"BFS {new_bfs}: found {len(predecessors)} predecessors: {predecessors}")

        # Get predecessor data from continuous features
        pred_data = cont_features[cont_features['bfs_nr'].isin(predecessors)]

        if len(pred_data) == 0:
            logger.warning(f"No feature data found for predecessors of BFS {new_bfs}")
            continue

        # Aggregate: population-weighted average for percentages, sum for counts
        total_pop = pred_data['einwohner'].sum()

        if total_pop == 0:
            logger.warning(f"Zero population for predecessors of BFS {new_bfs}")
            continue

        # Create aggregated row
        agg_row = {
            'bfs_nr': new_bfs,
            'gemeindename': voting_munis[voting_munis['bfs_nr'] == new_bfs]['geo_name'].iloc[0],
            'einwohner': total_pop,
        }

        # Population-weighted averages for percentage columns
        pct_cols = [col for col in pred_data.columns if 'anteil' in col or 'pct' in col
                    or 'dichte' in col or 'quote' in col or 'ziffer' in col or 'groesse' in col]
        for col in pct_cols:
            if col in pred_data.columns and pred_data[col].notna().any():
                weights = pred_data['einwohner'] / total_pop
                agg_row[col] = (pred_data[col] * weights).sum()

        # Sum for count columns
        count_cols = ['privathaushalte', 'einwohner']
        for col in count_cols:
            if col in pred_data.columns:
                agg_row[col] = pred_data[col].sum()

        # Copy other columns (take first non-null value)
        for col in pred_data.columns:
            if col not in agg_row and col not in ['bfs_nr', 'gemeindename']:
                valid_vals = pred_data[col].dropna()
                if len(valid_vals) > 0:
                    agg_row[col] = valid_vals.iloc[0]

        aggregated_rows.append(agg_row)

    logger.info(f"Created {len(aggregated_rows)} aggregated rows for fusion municipalities")

    # 6. Combine original and aggregated data
    if aggregated_rows:
        agg_df = pd.DataFrame(aggregated_rows)
        combined_features = pd.concat([cont_features, agg_df], ignore_index=True)
    else:
        combined_features = cont_features

    # 7. Merge with ESTV income data
    combined_features = combined_features.merge(estv_income, on='bfs_nr', how='left')

    # 8. Also aggregate ESTV data for fusion municipalities
    for new_bfs in missing_bfs:
        predecessors = mergers[mergers['new_bfs_number'] == new_bfs]['old_bfs_number'].tolist()
        pred_estv = estv_income[estv_income['bfs_nr'].isin(predecessors)]

        if len(pred_estv) > 0:
            # Simple average for income data
            idx = combined_features[combined_features['bfs_nr'] == new_bfs].index
            if len(idx) > 0:
                combined_features.loc[idx, 'steuerbares_einkommen_pro_kopf'] = pred_estv['steuerbares_einkommen_pro_kopf'].mean()
                combined_features.loc[idx, 'pct_einkommen_ueber_100k'] = pred_estv['pct_einkommen_ueber_100k'].mean()
                combined_features.loc[idx, 'pct_einkommen_unter_40k'] = pred_estv['pct_einkommen_unter_40k'].mean()

    # 9. Save to database
    conn.execute("DROP TABLE IF EXISTS municipality_features_complete")
    combined_features.to_sql('municipality_features_complete', conn, index=False)
    conn.execute("CREATE INDEX idx_complete_bfs ON municipality_features_complete(bfs_nr)")
    conn.commit()

    logger.info(f"Created municipality_features_complete with {len(combined_features)} rows")

    # 10. Verify coverage
    coverage = pd.read_sql_query("""
        SELECT
            COUNT(DISTINCT CAST(vr.geo_id AS INTEGER)) as covered,
            (SELECT COUNT(DISTINCT CAST(geo_id AS INTEGER))
             FROM voting_results
             WHERE CAST(geo_id AS INTEGER) > 100 AND CAST(geo_id AS INTEGER) < 9000
             AND geo_name NOT LIKE 'Bezirk%' AND geo_name NOT LIKE 'District%'
             AND geo_name NOT LIKE 'Distretto%' AND geo_name NOT LIKE 'Wahlkreis%'
             AND geo_name NOT LIKE 'Region%' AND geo_name NOT LIKE 'Kanton%'
             AND geo_name NOT LIKE 'Canton%') as total
        FROM voting_results vr
        WHERE EXISTS (SELECT 1 FROM municipality_features_complete mfc
                      WHERE mfc.bfs_nr = CAST(vr.geo_id AS INTEGER))
        AND CAST(vr.geo_id AS INTEGER) > 100 AND CAST(vr.geo_id AS INTEGER) < 9000
    """, conn)

    print("\n" + "="*60)
    print("COMPLETE FEATURES VIEW - SUMMARY")
    print("="*60)
    print(f"Total municipalities in features: {len(combined_features)}")
    print(f"Coverage of voting municipalities: {coverage['covered'].iloc[0]} / {coverage['total'].iloc[0]}")
    print(f"Coverage percentage: {100 * coverage['covered'].iloc[0] / coverage['total'].iloc[0]:.1f}%")
    print("\nKey columns:")
    for col in ['einwohner', 'bevoelkerungsdichte', 'auslaenderanteil',
                'anteil_65_plus_jahre', 'steuerbares_einkommen_pro_kopf']:
        if col in combined_features.columns:
            non_null = combined_features[col].notna().sum()
            print(f"  {col}: {non_null} values")
    print("="*60)

    conn.close()
    return combined_features


if __name__ == '__main__':
    create_complete_view()
