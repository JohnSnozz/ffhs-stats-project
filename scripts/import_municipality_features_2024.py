#!/usr/bin/env python3
"""
Import municipality features from new BFS Excel file (Raumgliederungen.xlsx) into SQLite database.

Source: Raumgliederungen.xlsx (2024 version)
- Sheet 'Daten': Municipality data with feature codes
- Other sheets: Label mappings for each feature

Creates tables:
- municipality_features_2024: BFS number + feature codes
- feature_labels_2024: Lookup table for code -> label mappings
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"import_features_2024_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths
EXCEL_FILE = Path('data/raw/features/Raumgliederungen.xlsx')
DB_FILE = Path('data/processed/swiss_votings.db')

# Feature columns (index 6 onwards)
FEATURE_START_COL = 6

# Mapping from feature column names to their label sheet names
FEATURE_TO_LABEL_SHEET = {
    'Grossregionen der Schweiz': 'CH1+CL_REGCH+2011.2',
    'Agglomerationen 2020': 'CH1+CL_AGGL+2020.0',
    'Agglomerationsgrössenklasse': 'CH1+CL_AGGLGK+2000.0',
    'Agglomerationen 2020.1': 'CH1+CL_AGGL+2020.0',
    'Raum mit städtischem Charakter': 'CH1+CL_RSTCKT+2014.2',
    'Statistische Städte': 'CH1+CL_STADTE+2014.2',
    'Stadt/Land-Typologie': 'CH1+CL_STALAN+2012.1',
    'Gemeindetypologie (9 Typen)': 'CH1+CL_GDET9+2012.1',
    'Gemeindetypologie (25 Typen)': 'CH1+CL_GDET25+2012.1',
    'Arbeitsmarktgrossregionen 2018': 'CH1+CL_GBAE+2018.0',
    'Arbeitsmarktregionen 2018': 'CH1+CL_BAE+2018.0',
    'Sprachgebiete': 'CH1+CL_SPRGEB+2011.2',
    'Berggebiete': 'CH1+CL_MONT+1.0',
    'Urbanisierungsgrad (DEGURBA) - eurostat': 'CH1+CL_DEGURB+2017.1',
    'Urbanisierungsgrad (DEGURBA) - eurostat.1': 'CH1+CL_DEGURB+2017.1',
    'Erweiterte Städte 2021 (Greater cities eurostat)': 'CH1+CL_GCITIES+2021.0',
    'Erweiterte Städte 2011 (Greater cities eurostat)': 'CH1+CL_GCITIES+2011.0',
    'Funktionale städtische Gebiete 2021 (FUA eurostat)': 'CH1+CL_FUA+2021.0',
    'Funktionale städtische Gebiete 2014 (FUA eurostat)': 'CH1+CL_FUA+2014.0',
}


def load_municipality_data(xlsx: pd.ExcelFile) -> tuple[pd.DataFrame, dict]:
    """Load municipality data from 'Daten' sheet."""
    logger.info("Loading municipality data from 'Daten' sheet...")

    # Header is in row 2 (index 1), skip row 3 (metadata codes)
    df = pd.read_excel(xlsx, sheet_name='Daten', header=1, skiprows=[2])

    # Rename columns for cleaner database storage
    column_mapping = {
        'BFS Gde-nummer': 'bfs_nr',
        'Gemeindename': 'gemeindename',
        'Kantons-nummer': 'kanton_nr',
        'Kanton': 'kanton',
        'Bezirks-nummer': 'bezirk_nr',
        'Bezirksname': 'bezirksname',
    }
    df = df.rename(columns=column_mapping)

    # Clean feature column names (remove special chars, make lowercase)
    feature_cols = df.columns[FEATURE_START_COL:]
    clean_names = {}
    for col in feature_cols:
        original = col
        clean = col.lower()
        clean = clean.replace(' ', '_')
        clean = clean.replace('/', '_')
        clean = clean.replace('-', '_')
        clean = clean.replace('(', '').replace(')', '')
        clean = clean.replace('.', '_')
        clean = clean.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
        # Remove duplicate underscores
        while '__' in clean:
            clean = clean.replace('__', '_')
        clean_names[original] = clean

    df = df.rename(columns=clean_names)

    # Convert BFS number to int
    df['bfs_nr'] = df['bfs_nr'].astype(int)

    logger.info(f"Loaded {len(df)} municipalities with {len(feature_cols)} features")
    return df, clean_names


def load_label_mappings(xlsx: pd.ExcelFile) -> dict:
    """Load label mappings from all label sheets."""
    logger.info("Loading label mappings...")

    all_labels = {}

    for feature_name, sheet_name in FEATURE_TO_LABEL_SHEET.items():
        try:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)

            if len(df) > 0:
                # Skip the header row
                df = df.iloc[1:].copy()
                df.columns = ['code', 'label']
                df['code'] = pd.to_numeric(df['code'], errors='coerce')
                df = df.dropna(subset=['code'])
                df['code'] = df['code'].astype(int)

                mapping = dict(zip(df['code'], df['label']))
                all_labels[feature_name] = mapping
                logger.debug(f"  {feature_name}: {len(mapping)} labels")
        except Exception as e:
            logger.warning(f"Could not load labels for {feature_name}: {e}")

    logger.info(f"Loaded labels for {len(all_labels)} features")
    return all_labels


def create_tables(conn: sqlite3.Connection):
    """Create database tables for municipality features."""
    cursor = conn.cursor()

    # Drop existing tables (only the 2024 versions)
    cursor.execute("DROP TABLE IF EXISTS municipality_features_2024")
    cursor.execute("DROP TABLE IF EXISTS feature_labels_2024")

    conn.commit()
    logger.info("Dropped existing 2024 feature tables")


def import_features(conn: sqlite3.Connection, df: pd.DataFrame, clean_names: dict):
    """Import municipality features into database."""
    logger.info("Importing municipality features...")

    # Select columns to import: identifier columns + feature columns
    id_cols = ['bfs_nr', 'gemeindename', 'kanton_nr', 'kanton', 'bezirk_nr', 'bezirksname']
    feature_cols = list(clean_names.values())

    cols_to_import = id_cols + feature_cols
    df_import = df[cols_to_import].copy()

    # Import to database
    df_import.to_sql('municipality_features_2024', conn, if_exists='replace', index=False)

    # Create index on BFS number
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mf2024_bfs_nr ON municipality_features_2024(bfs_nr)")
    conn.commit()

    logger.info(f"Imported {len(df_import)} municipalities with {len(feature_cols)} features")
    return feature_cols


def import_labels(conn: sqlite3.Connection, labels: dict, clean_names: dict):
    """Import feature labels into database."""
    logger.info("Importing feature labels...")

    rows = []
    for original_name, mapping in labels.items():
        clean_name = clean_names.get(original_name, original_name)
        for code, label in mapping.items():
            rows.append({
                'feature_name': clean_name,
                'feature_original_name': original_name,
                'code': code,
                'label': label
            })

    df_labels = pd.DataFrame(rows)
    df_labels.to_sql('feature_labels_2024', conn, if_exists='replace', index=False)

    # Create indexes
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fl2024_feature ON feature_labels_2024(feature_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fl2024_code ON feature_labels_2024(feature_name, code)")
    conn.commit()

    logger.info(f"Imported {len(df_labels)} label mappings")


def print_summary(conn: sqlite3.Connection):
    """Print summary of imported data."""
    cursor = conn.cursor()

    print("\n" + "="*60)
    print("IMPORT SUMMARY (2024 Features)")
    print("="*60)

    # Count municipalities
    cursor.execute("SELECT COUNT(*) FROM municipality_features_2024")
    muni_count = cursor.fetchone()[0]
    print(f"Municipalities imported: {muni_count}")

    # Count features (columns - 6 identifier columns)
    cursor.execute("PRAGMA table_info(municipality_features_2024)")
    cols = cursor.fetchall()
    feature_count = len(cols) - 6
    print(f"Features imported: {feature_count}")

    # Count labels
    cursor.execute("SELECT COUNT(*) FROM feature_labels_2024")
    label_count = cursor.fetchone()[0]
    print(f"Label mappings imported: {label_count}")

    # Show feature columns
    print("\nFeature columns:")
    for col in cols[6:]:
        print(f"  - {col[1]}")

    # Sample data
    print("\nSample data (Zürich):")
    cursor.execute("""
        SELECT bfs_nr, gemeindename, kanton, sprachgebiete, grossregionen_der_schweiz
        FROM municipality_features_2024
        WHERE gemeindename = 'Zürich'
    """)
    row = cursor.fetchone()
    if row:
        print(f"  BFS: {row[0]}, Name: {row[1]}, Kanton: {row[2]}")
        print(f"  Sprachgebiet: {row[3]}, Grossregion: {row[4]}")

    print("="*60)


def main():
    logger.info("="*60)
    logger.info("Starting municipality features import (2024)")
    logger.info("="*60)

    # Check files exist
    if not EXCEL_FILE.exists():
        logger.error(f"Excel file not found: {EXCEL_FILE}")
        return

    if not DB_FILE.exists():
        logger.error(f"Database not found: {DB_FILE}")
        return

    # Load Excel file
    logger.info(f"Loading Excel file: {EXCEL_FILE}")
    xlsx = pd.ExcelFile(EXCEL_FILE)

    # Load data
    df, clean_names = load_municipality_data(xlsx)
    labels = load_label_mappings(xlsx)

    # Connect to database
    logger.info(f"Connecting to database: {DB_FILE}")
    conn = sqlite3.connect(DB_FILE)

    try:
        # Create tables and import data
        create_tables(conn)
        import_features(conn, df, clean_names)
        import_labels(conn, labels, clean_names)

        # Print summary
        print_summary(conn)

        logger.info("Import completed successfully!")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
