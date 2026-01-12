#!/usr/bin/env python3
"""
Import ESTV (Eidgenössische Steuerverwaltung) income data.

Source: Direkte Bundessteuer, Natürliche Personen, Gemeinden
Data: Steuerbares Einkommen (taxable income) 2020
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path(__file__).parent.parent / 'data'
XLSX_PATH = DATA_DIR / 'raw' / 'features' / 'estv_einkommen_2020.xlsx'
DB_PATH = DATA_DIR / 'processed' / 'swiss_votings.db'


def load_estv_data():
    """Load and process ESTV income data."""
    logger.info(f"Loading ESTV data from {XLSX_PATH}")

    # Sheet 511: Number of taxpayers by income class
    df_taxpayers = pd.read_excel(XLSX_PATH, sheet_name='511', header=2)

    # Sheet 512: Taxable income by income class
    df_income = pd.read_excel(XLSX_PATH, sheet_name='512', header=2)

    # Clean column names
    df_taxpayers.columns = ['kanton_id', 'kanton', 'bfs_nr', 'gemeinde',
                            'klasse_0', 'klasse_1_30k', 'klasse_30k_40k', 'klasse_40k_50k',
                            'klasse_50k_75k', 'klasse_75k_100k', 'klasse_100k_200k',
                            'klasse_200k_500k', 'klasse_500k_1m', 'klasse_1m_plus', 'total']

    df_income.columns = ['kanton_id', 'kanton', 'bfs_nr', 'gemeinde',
                         'klasse_0', 'klasse_1_30k', 'klasse_30k_40k', 'klasse_40k_50k',
                         'klasse_50k_75k', 'klasse_75k_100k', 'klasse_100k_200k',
                         'klasse_200k_500k', 'klasse_500k_1m', 'klasse_1m_plus', 'total']

    # Forward fill canton info
    df_taxpayers['kanton_id'] = df_taxpayers['kanton_id'].ffill()
    df_taxpayers['kanton'] = df_taxpayers['kanton'].ffill()
    df_income['kanton_id'] = df_income['kanton_id'].ffill()
    df_income['kanton'] = df_income['kanton'].ffill()

    # Filter to municipality rows only (have bfs_nr)
    df_taxpayers = df_taxpayers[df_taxpayers['bfs_nr'].notna()].copy()
    df_income = df_income[df_income['bfs_nr'].notna()].copy()

    # Convert bfs_nr to integer
    df_taxpayers['bfs_nr'] = df_taxpayers['bfs_nr'].astype(int)
    df_income['bfs_nr'] = df_income['bfs_nr'].astype(int)

    # Replace '-' with 0
    for col in df_taxpayers.columns[4:]:
        df_taxpayers[col] = pd.to_numeric(df_taxpayers[col].replace('-', 0), errors='coerce').fillna(0)
        df_income[col] = pd.to_numeric(df_income[col].replace('-', 0), errors='coerce').fillna(0)

    logger.info(f"Loaded {len(df_taxpayers)} municipalities (taxpayers)")
    logger.info(f"Loaded {len(df_income)} municipalities (income)")

    # Calculate average income per taxpayer
    df_result = pd.DataFrame({
        'bfs_nr': df_income['bfs_nr'],
        'gemeinde': df_income['gemeinde'],
        'kanton': df_income['kanton'],
        'steuerbares_einkommen_total': df_income['total'],
        'steuerpflichtige_total': df_taxpayers['total'],
    })

    # Calculate average
    df_result['steuerbares_einkommen_pro_kopf'] = (
        df_result['steuerbares_einkommen_total'] / df_result['steuerpflichtige_total']
    ).round(0)

    # Handle division by zero
    df_result['steuerbares_einkommen_pro_kopf'] = df_result['steuerbares_einkommen_pro_kopf'].replace(
        [float('inf'), float('-inf')], pd.NA
    )

    # Add income distribution percentages
    for col in ['klasse_1_30k', 'klasse_30k_40k', 'klasse_40k_50k', 'klasse_50k_75k',
                'klasse_75k_100k', 'klasse_100k_200k', 'klasse_200k_500k',
                'klasse_500k_1m', 'klasse_1m_plus']:
        pct_col = f'pct_{col}'
        df_result[pct_col] = (df_taxpayers[col] / df_taxpayers['total'] * 100).round(2)

    # Calculate high income share (>100k)
    df_result['pct_einkommen_ueber_100k'] = (
        (df_taxpayers['klasse_100k_200k'] + df_taxpayers['klasse_200k_500k'] +
         df_taxpayers['klasse_500k_1m'] + df_taxpayers['klasse_1m_plus']) /
        df_taxpayers['total'] * 100
    ).round(2)

    # Calculate low income share (<40k)
    df_result['pct_einkommen_unter_40k'] = (
        (df_taxpayers['klasse_1_30k'] + df_taxpayers['klasse_30k_40k']) /
        df_taxpayers['total'] * 100
    ).round(2)

    return df_result


def import_to_sqlite(df):
    """Import to SQLite database."""
    logger.info(f"Connecting to {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    # Create table
    conn.execute("DROP TABLE IF EXISTS estv_income_2020")

    df.to_sql('estv_income_2020', conn, index=False, if_exists='replace')

    # Create index
    conn.execute("CREATE INDEX IF NOT EXISTS idx_estv_bfs ON estv_income_2020(bfs_nr)")

    conn.commit()

    # Verify
    count = pd.read_sql_query("SELECT COUNT(*) as n FROM estv_income_2020", conn)
    logger.info(f"Imported {count['n'].iloc[0]} rows to estv_income_2020")

    conn.close()


def print_summary(df):
    """Print summary statistics."""
    print("\n" + "="*60)
    print("ESTV INCOME DATA 2020 - IMPORT SUMMARY")
    print("="*60)
    print(f"Municipalities: {len(df)}")
    print(f"\nKey statistics:")
    print(f"  Avg income per taxpayer: CHF {df['steuerbares_einkommen_pro_kopf'].mean():,.0f}")
    print(f"  Median income per taxpayer: CHF {df['steuerbares_einkommen_pro_kopf'].median():,.0f}")
    print(f"  Min: CHF {df['steuerbares_einkommen_pro_kopf'].min():,.0f}")
    print(f"  Max: CHF {df['steuerbares_einkommen_pro_kopf'].max():,.0f}")
    print(f"\nIncome distribution (avg across municipalities):")
    print(f"  < 40k: {df['pct_einkommen_unter_40k'].mean():.1f}%")
    print(f"  > 100k: {df['pct_einkommen_ueber_100k'].mean():.1f}%")
    print("\nSample (top 5 by income):")
    print(df.nlargest(5, 'steuerbares_einkommen_pro_kopf')[
        ['bfs_nr', 'gemeinde', 'kanton', 'steuerbares_einkommen_pro_kopf']
    ].to_string())
    print("="*60)


def main():
    df = load_estv_data()
    import_to_sqlite(df)
    print_summary(df)
    return df


if __name__ == '__main__':
    main()
