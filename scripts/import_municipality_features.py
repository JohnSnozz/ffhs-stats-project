#!/usr/bin/env python3
"""
Import municipality features from BFS Excel file into SQLite database.

Source: be-d-00.04-rgs-01.xlsx (Regionalstatistisches Gemeindeverzeichnis)
- Sheet 'Daten': Municipality data with feature codes
- Other sheets: Label mappings for each feature

Creates tables:
- municipality_features: BFS number + feature codes (columns G-AD)
- feature_labels: Lookup table for code -> label mappings
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"import_features_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
EXCEL_FILE = Path('data/raw/features/be-d-00.04-rgs-01.xlsx')
DB_FILE = Path('data/processed/swiss_votings.db')

# Feature columns (G to AD = index 6 to 29)
FEATURE_START_COL = 6
FEATURE_END_COL = 30  # exclusive

# Mapping from feature column names to their label sheet names
# Based on the Excel structure
FEATURE_TO_LABEL_SHEET = {
    'Grossregionen der Schweiz': 'CH1+CL_REGCH+2011.2',
    'Agglomerationen 2012': 'CH1+CL_AGGL+2012.0',
    'Agglomerationsgrössenklasse': 'CH1+CL_AGGLGK+2000.0',
    'Agglomerationen 2012.1': 'CH1+CL_AGGL+2012.0',  # Same as Agglomerationen 2012
    'Raum mit städtischem Charakter 2012': 'CH1+CL_RSTCKT+2014.2',
    'Statistische Städte 2012': 'CH1+CL_STADTE+2014.2',
    'Städtische / Ländliche Gebiete': 'CH1+CL_STALAN+2012.1',
    'Gemeindetypologie 2012 (9 Typen)': 'CH1+CL_GDET9+2012.1',
    'Gemeindetypologie 2012 (25 Typen)': 'CH1+CL_GDET25+2012.1',
    'Arbeitsmarktgrossregionen 2018': 'CH1+CL_GBAE2018+1.0',
    'Arbeitsmarktregionen 2018': 'CH1+CL_BAE2018+1.0',
    'Urbanisierungsgrad (DEGURBA) - eurostat': 'CH1+CL_DEGURB+2017.1',
    'Sprachgebiete': 'CH1+CL_SPRGEB+2011.2',
    'Metropolitanregionen': 'CH1+CL_METROR+2011.2',
    'Agglomerationen 2000': 'CH1+CL_AGGL+2000.0',
    'Städtische / Ländliche Gebiete.1': 'CH1+CL_STALAN+2011.2',
    'Gemeindetypologie 1980-2000 (9 Typen)': 'CH1+CL_GDET9+2011.2',
    'Gemeindetypologie 1980-2000 (22 Typen)': 'CH1+CL_GDET22+2011.2',
    'MS Regionen': 'CH1+CL_MSREG+2011.2',
    'Arbeitsmarktregionen': 'CH1+CL_ARBREG+2011.2',
    'MS Regionen.1': 'CH1+CL_MSREG+2011.2',  # Same as MS Regionen
    'Typologie der MS-Regionen': 'CH1+CL_TYPMSR+2011.2',
    'MS Regionen.2': 'CH1+CL_MSREG+2011.2',  # Same as MS Regionen
    'Europäische Berggebietsregionen': 'CH1+CL_BERGEB+2017.1',
}


def load_municipality_data(xlsx: pd.ExcelFile) -> pd.DataFrame:
    """Load municipality data from 'Daten' sheet."""
    logger.info("Loading municipality data from 'Daten' sheet...")

    # Header is in row 2 (index 1), skip row 3 (links)
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
    feature_cols = df.columns[FEATURE_START_COL:FEATURE_END_COL]
    clean_names = {}
    for col in feature_cols:
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
        clean_names[col] = clean

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

            # The structure is: first column = Code (with header in row 1), second column = Label
            # Row 0 has column names, row 1+ has data
            if len(df) > 0:
                # Skip the header row that says "Code" / "Label"
                df = df.iloc[1:].copy()
                df.columns = ['code', 'label']
                df['code'] = pd.to_numeric(df['code'], errors='coerce')
                df = df.dropna(subset=['code'])
                df['code'] = df['code'].astype(int)

                # Store as dict: code -> label
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

    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS municipality_features")
    cursor.execute("DROP TABLE IF EXISTS feature_labels")

    conn.commit()
    logger.info("Dropped existing feature tables")


def import_features(conn: sqlite3.Connection, df: pd.DataFrame, clean_names: dict):
    """Import municipality features into database."""
    logger.info("Importing municipality features...")

    # Select columns to import: identifier columns + feature columns
    id_cols = ['bfs_nr', 'gemeindename', 'kanton_nr', 'kanton', 'bezirk_nr', 'bezirksname']
    feature_cols = [clean_names[col] for col in list(clean_names.keys())]

    cols_to_import = id_cols + feature_cols
    df_import = df[cols_to_import].copy()

    # Import to database
    df_import.to_sql('municipality_features', conn, if_exists='replace', index=False)

    # Create index on BFS number
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mf_bfs_nr ON municipality_features(bfs_nr)")
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
    df_labels.to_sql('feature_labels', conn, if_exists='replace', index=False)

    # Create indexes
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fl_feature ON feature_labels(feature_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fl_code ON feature_labels(feature_name, code)")
    conn.commit()

    logger.info(f"Imported {len(df_labels)} label mappings")


def create_feature_view(conn: sqlite3.Connection):
    """Create a view that joins features with labels for easy querying."""
    logger.info("Creating feature lookup view...")

    cursor = conn.cursor()
    cursor.execute("DROP VIEW IF EXISTS v_municipality_features_labeled")

    # This view provides a way to get labeled values
    # For now, keep the raw feature table as primary
    cursor.execute("""
        CREATE VIEW v_municipality_features_labeled AS
        SELECT
            mf.*,
            fl_sprach.label as sprachgebiet_label,
            fl_gross.label as grossregion_label,
            fl_urban.label as urbanisierungsgrad_label,
            fl_gemtyp9.label as gemeindetypologie_9_label
        FROM municipality_features mf
        LEFT JOIN feature_labels fl_sprach
            ON fl_sprach.feature_name = 'sprachgebiete'
            AND fl_sprach.code = mf.sprachgebiete
        LEFT JOIN feature_labels fl_gross
            ON fl_gross.feature_name = 'grossregionen_der_schweiz'
            AND fl_gross.code = mf.grossregionen_der_schweiz
        LEFT JOIN feature_labels fl_urban
            ON fl_urban.feature_name = 'urbanisierungsgrad_degurba_eurostat'
            AND fl_urban.code = mf.urbanisierungsgrad_degurba_eurostat
        LEFT JOIN feature_labels fl_gemtyp9
            ON fl_gemtyp9.feature_name = 'gemeindetypologie_2012_9_typen'
            AND fl_gemtyp9.code = mf.gemeindetypologie_2012_9_typen
    """)

    conn.commit()
    logger.info("Created v_municipality_features_labeled view")


def print_summary(conn: sqlite3.Connection):
    """Print summary of imported data."""
    cursor = conn.cursor()

    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)

    # Count municipalities
    cursor.execute("SELECT COUNT(*) FROM municipality_features")
    muni_count = cursor.fetchone()[0]
    print(f"Municipalities imported: {muni_count}")

    # Count features (columns - 6 identifier columns)
    cursor.execute("PRAGMA table_info(municipality_features)")
    cols = cursor.fetchall()
    feature_count = len(cols) - 6
    print(f"Features imported: {feature_count}")

    # Count labels
    cursor.execute("SELECT COUNT(*) FROM feature_labels")
    label_count = cursor.fetchone()[0]
    print(f"Label mappings imported: {label_count}")

    # Show feature columns
    print("\nFeature columns:")
    for col in cols[6:]:
        print(f"  - {col[1]}")

    # Sample data
    print("\nSample data (Zürich):")
    cursor.execute("""
        SELECT bfs_nr, gemeindename, kanton, sprachgebiete, grossregionen_der_schweiz,
               urbanisierungsgrad_degurba_eurostat
        FROM municipality_features
        WHERE gemeindename = 'Zürich'
    """)
    row = cursor.fetchone()
    if row:
        print(f"  BFS: {row[0]}, Name: {row[1]}, Kanton: {row[2]}")
        print(f"  Sprachgebiet: {row[3]}, Grossregion: {row[4]}, Urbanisierung: {row[5]}")

    print("="*60)


def main():
    logger.info("="*60)
    logger.info("Starting municipality features import")
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
        create_feature_view(conn)

        # Print summary
        print_summary(conn)

        logger.info("Import completed successfully!")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
