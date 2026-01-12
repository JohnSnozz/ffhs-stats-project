#!/usr/bin/env python3
"""
Import continuous municipality features from BFS Regionalportraets 2021.

Creates table 'municipality_continuous_features' with pivot data for all municipalities.

Indicators:
- Ind_01_01: Einwohner (Population)
- Ind_01_03: Bevölkerungsdichte (Population density per km²)
- Ind_01_04: Anteil 0-19 Jahre (%)
- Ind_01_05: Anteil 20-64 Jahre (%)
- Ind_01_06: Anteil 65+ Jahre (%)
- Ind_01_08: Ausländeranteil (%)
- Ind_01_11: Geburtenziffer (per 1000)
- Ind_01_12: Sterbeziffer (per 1000)
- Ind_01_14: Durchschnittliche Haushaltsgrösse
- Ind_04_01: Fläche Total (ha)
- Ind_04_02: Siedlungsfläche (%)
- Ind_06_03: Beschäftigte Total
- Ind_11_01: Sozialhilfequote (%)
- Ind_14_01-10: Wähleranteile Parteien (%)
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path(__file__).parent.parent / 'data'
CSV_PATH = DATA_DIR / 'raw' / 'features' / 'regionalportraets_2021_master.csv'
DB_PATH = DATA_DIR / 'processed' / 'swiss_votings.db'

# Indicator mapping (code -> readable name)
INDICATOR_NAMES = {
    'Ind_01_01': 'einwohner',
    'Ind_01_02': 'einwohner_veraenderung_pct',
    'Ind_01_03': 'bevoelkerungsdichte',
    'Ind_01_04': 'anteil_0_19_jahre',
    'Ind_01_05': 'anteil_20_64_jahre',
    'Ind_01_06': 'anteil_65_plus_jahre',
    'Ind_01_08': 'auslaenderanteil',
    'Ind_01_09': 'heiratsziffer',
    'Ind_01_10': 'scheidungsziffer',
    'Ind_01_11': 'geburtenziffer',
    'Ind_01_12': 'sterbeziffer',
    'Ind_01_13': 'privathaushalte',
    'Ind_01_14': 'haushaltsgroesse',
    'Ind_04_01': 'flaeche_total_ha',
    'Ind_04_02': 'siedlungsflaeche_pct',
    'Ind_04_03': 'siedlungsflaeche_veraenderung',
    'Ind_04_04': 'landwirtschaftsflaeche_pct',
    'Ind_04_05': 'landwirtschaftsflaeche_veraenderung',
    'Ind_04_06': 'wald_pct',
    'Ind_04_07': 'unproduktive_flaeche_pct',
    'Ind_06_03': 'beschaeftigte_total',
    'Ind_06_04': 'beschaeftigte_sektor1',
    'Ind_06_05': 'beschaeftigte_sektor2',
    'Ind_06_06': 'beschaeftigte_sektor3',
    'Ind_06_07': 'arbeitsstaetten_total',
    'Ind_06_08': 'arbeitsstaetten_sektor1',
    'Ind_06_09': 'arbeitsstaetten_sektor2',
    'Ind_06_10': 'arbeitsstaetten_sektor3',
    'Ind_08_01': 'leerwohnungsziffer',
    'Ind_08_04': 'neue_wohnungen',
    'Ind_11_01': 'sozialhilfequote',
    'Ind_14_01': 'waehleranteil_fdp',
    'Ind_14_02': 'waehleranteil_cvp',
    'Ind_14_03': 'waehleranteil_sp',
    'Ind_14_04': 'waehleranteil_svp',
    'Ind_14_05': 'waehleranteil_evp_csp',
    'Ind_14_06': 'waehleranteil_glp',
    'Ind_14_07': 'waehleranteil_bdp',
    'Ind_14_08': 'waehleranteil_gps',
    'Ind_14_09': 'waehleranteil_andere',
    'Ind_14_10': 'waehleranteil_klein',
}

# Indicator descriptions for labels table
INDICATOR_DESCRIPTIONS = {
    'einwohner': 'Anzahl Einwohner (ständige Wohnbevölkerung)',
    'einwohner_veraenderung_pct': 'Bevölkerungsveränderung in %',
    'bevoelkerungsdichte': 'Einwohner pro km² Gesamtfläche',
    'anteil_0_19_jahre': 'Anteil Altersgruppe 0-19 Jahre in %',
    'anteil_20_64_jahre': 'Anteil Altersgruppe 20-64 Jahre in %',
    'anteil_65_plus_jahre': 'Anteil Altersgruppe 65+ Jahre in %',
    'auslaenderanteil': 'Ausländeranteil in %',
    'heiratsziffer': 'Rohe Heiratsziffer (pro 1000 Einwohner)',
    'scheidungsziffer': 'Rohe Scheidungsziffer (pro 1000 Einwohner)',
    'geburtenziffer': 'Rohe Geburtenziffer (pro 1000 Einwohner)',
    'sterbeziffer': 'Rohe Sterbeziffer (pro 1000 Einwohner)',
    'privathaushalte': 'Anzahl Privathaushalte',
    'haushaltsgroesse': 'Durchschnittliche Haushaltsgrösse',
    'flaeche_total_ha': 'Gesamtfläche in Hektaren',
    'siedlungsflaeche_pct': 'Siedlungsfläche in %',
    'siedlungsflaeche_veraenderung': 'Siedlungsfläche Veränderung in %',
    'landwirtschaftsflaeche_pct': 'Landwirtschaftsfläche in %',
    'landwirtschaftsflaeche_veraenderung': 'Landwirtschaftsfläche Veränderung in %',
    'wald_pct': 'Wald und Gehölze in %',
    'unproduktive_flaeche_pct': 'Unproduktive Fläche in %',
    'beschaeftigte_total': 'Beschäftigte Total',
    'beschaeftigte_sektor1': 'Beschäftigte im 1. Sektor (Landwirtschaft)',
    'beschaeftigte_sektor2': 'Beschäftigte im 2. Sektor (Industrie)',
    'beschaeftigte_sektor3': 'Beschäftigte im 3. Sektor (Dienstleistungen)',
    'arbeitsstaetten_total': 'Arbeitsstätten Total',
    'arbeitsstaetten_sektor1': 'Arbeitsstätten im 1. Sektor',
    'arbeitsstaetten_sektor2': 'Arbeitsstätten im 2. Sektor',
    'arbeitsstaetten_sektor3': 'Arbeitsstätten im 3. Sektor',
    'leerwohnungsziffer': 'Leerwohnungsziffer in %',
    'neue_wohnungen': 'Neu gebaute Wohnungen (pro 1000 Einwohner)',
    'sozialhilfequote': 'Sozialhilfequote in %',
    'waehleranteil_fdp': 'Wähleranteil FDP (NR-Wahlen) in %',
    'waehleranteil_cvp': 'Wähleranteil CVP/Mitte (NR-Wahlen) in %',
    'waehleranteil_sp': 'Wähleranteil SP (NR-Wahlen) in %',
    'waehleranteil_svp': 'Wähleranteil SVP (NR-Wahlen) in %',
    'waehleranteil_evp_csp': 'Wähleranteil EVP/CSP (NR-Wahlen) in %',
    'waehleranteil_glp': 'Wähleranteil GLP (NR-Wahlen) in %',
    'waehleranteil_bdp': 'Wähleranteil BDP (NR-Wahlen) in %',
    'waehleranteil_gps': 'Wähleranteil GPS/Grüne (NR-Wahlen) in %',
    'waehleranteil_andere': 'Wähleranteil andere Parteien in %',
    'waehleranteil_klein': 'Wähleranteil Kleinparteien in %',
}


def load_and_transform_data():
    """Load CSV and pivot to wide format."""
    logger.info(f"Loading data from {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig', low_memory=False)
    logger.info(f"Loaded {len(df)} rows")

    # Filter to most recent year for each indicator
    # Use 2019 as the main reference year (most complete)
    df_recent = df[df['PERIOD_REF'] == '2019'].copy()

    # Also get area data which uses different period
    df_area = df[df['PERIOD_REF'] == '2004/2009'].copy()
    df_area = df_area[df_area['INDICATORS'].isin(['Ind_04_01', 'Ind_04_02', 'Ind_04_04', 'Ind_04_06', 'Ind_04_07'])]

    # Get voter data (2019 National Council elections)
    df_voters = df[df['PERIOD_REF'] == '2019'].copy()
    df_voters = df_voters[df_voters['INDICATORS'].str.startswith('Ind_14')]

    # Combine
    df_combined = pd.concat([df_recent, df_area, df_voters]).drop_duplicates(
        subset=['CODE_REGION', 'INDICATORS'], keep='first'
    )

    # Exclude CH (national level) and keep only municipality codes
    df_combined = df_combined[df_combined['CODE_REGION'] != 'CH']
    df_combined = df_combined[df_combined['CODE_REGION'].str.isnumeric()]

    logger.info(f"After filtering: {len(df_combined)} rows")

    # Rename indicators to readable names
    df_combined['indicator_name'] = df_combined['INDICATORS'].map(INDICATOR_NAMES)

    # Pivot to wide format
    df_pivot = df_combined.pivot_table(
        index=['CODE_REGION', 'REGION'],
        columns='indicator_name',
        values='VALUE',
        aggfunc='first'
    ).reset_index()

    # Rename columns
    df_pivot.columns.name = None
    df_pivot = df_pivot.rename(columns={
        'CODE_REGION': 'bfs_nr',
        'REGION': 'gemeindename'
    })

    # Convert bfs_nr to integer
    df_pivot['bfs_nr'] = df_pivot['bfs_nr'].astype(int)

    logger.info(f"Pivoted to {len(df_pivot)} municipalities with {len(df_pivot.columns)} columns")

    return df_pivot


def create_labels_table():
    """Create dataframe for feature labels."""
    labels = []
    for name, description in INDICATOR_DESCRIPTIONS.items():
        labels.append({
            'feature_name': name,
            'description': description
        })
    return pd.DataFrame(labels)


def import_to_sqlite(df, labels_df):
    """Import dataframes to SQLite."""
    logger.info(f"Connecting to {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    # Drop existing tables
    conn.execute("DROP TABLE IF EXISTS municipality_continuous_features")
    conn.execute("DROP TABLE IF EXISTS continuous_feature_labels")

    # Import main data
    df.to_sql('municipality_continuous_features', conn, index=False, if_exists='replace')
    logger.info(f"Imported {len(df)} rows to municipality_continuous_features")

    # Import labels
    labels_df.to_sql('continuous_feature_labels', conn, index=False, if_exists='replace')
    logger.info(f"Imported {len(labels_df)} labels to continuous_feature_labels")

    # Create index
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cont_features_bfs ON municipality_continuous_features(bfs_nr)")

    conn.commit()
    conn.close()

    logger.info("Import completed successfully")


def print_summary(df):
    """Print summary statistics."""
    print("\n" + "="*60)
    print("IMPORT SUMMARY - Continuous Features")
    print("="*60)
    print(f"Municipalities: {len(df)}")
    print(f"Features: {len(df.columns) - 2}")  # Exclude bfs_nr and gemeindename
    print("\nFeature columns:")
    for col in df.columns:
        if col not in ['bfs_nr', 'gemeindename']:
            non_null = df[col].notna().sum()
            print(f"  - {col}: {non_null} values")
    print("\nSample (first 5 rows):")
    print(df[['bfs_nr', 'gemeindename', 'einwohner', 'bevoelkerungsdichte', 'auslaenderanteil']].head())
    print("="*60)


def main():
    # Load and transform
    df = load_and_transform_data()
    labels_df = create_labels_table()

    # Import to SQLite
    import_to_sqlite(df, labels_df)

    # Print summary
    print_summary(df)

    return df


if __name__ == '__main__':
    main()
