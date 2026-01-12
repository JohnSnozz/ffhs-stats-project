#!/usr/bin/env python3
"""
Import Swiss municipal changes (Gemeindefusionen) from Excel into SQLite database.
This script handles mergers, splits, and name changes of municipalities over time.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
import logging
import json
import sys

# Setup logging
def setup_logging():
    """Setup logging configuration"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'municipal_changes_{timestamp}.log'

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

def read_excel_with_validation(excel_path, logger):
    """Read and validate Excel file with municipal changes"""
    logger.info(f"Reading Excel file: {excel_path}")

    try:
        # Read Excel with proper header handling (skip first row, use second as header)
        df = pd.read_excel(excel_path, sheet_name='Daten', header=1)
        logger.info(f"Successfully read Excel. Shape: {df.shape}")

        # Log column names for debugging
        logger.debug(f"Columns found: {list(df.columns)}")

        # Rename columns to standard names
        column_mapping = {
            df.columns[0]: 'mutation_number',
            df.columns[1]: 'old_canton',
            df.columns[2]: 'old_district_number',
            df.columns[3]: 'old_bfs_number',
            df.columns[4]: 'old_name',
            df.columns[5]: 'new_canton',
            df.columns[6]: 'new_district_number',
            df.columns[7]: 'new_bfs_number',
            df.columns[8]: 'new_name',
            df.columns[9]: 'mutation_date'
        }

        df = df.rename(columns=column_mapping)
        logger.info("Columns renamed successfully")

        # Remove header row if it exists in data
        if len(df) > 0 and str(df.iloc[0]['mutation_number']).lower() == 'mutationsnummer':
            logger.warning("Found header row in data, removing it")
            df = df[df['mutation_number'] != 'Mutationsnummer'].reset_index(drop=True)

        # Validate BFS numbers
        non_numeric_old = df[pd.to_numeric(df['old_bfs_number'], errors='coerce').isna() & df['old_bfs_number'].notna()]
        non_numeric_new = df[pd.to_numeric(df['new_bfs_number'], errors='coerce').isna() & df['new_bfs_number'].notna()]

        if len(non_numeric_old) > 0:
            logger.warning(f"Found {len(non_numeric_old)} rows with non-numeric old_bfs_number")
        if len(non_numeric_new) > 0:
            logger.warning(f"Found {len(non_numeric_new)} rows with non-numeric new_bfs_number")

        logger.info(f"Data validation complete. Final shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        raise

def analyze_changes(df, logger):
    """Analyze types of municipal changes"""
    logger.info("Analyzing change types...")

    def get_change_type(row):
        """Determine the type of municipal change"""
        changes = []

        if pd.notna(row['old_bfs_number']) and pd.notna(row['new_bfs_number']):
            if str(row['old_bfs_number']) != str(row['new_bfs_number']):
                changes.append('bfs_change')

        if pd.notna(row['old_name']) and pd.notna(row['new_name']):
            if row['old_name'] != row['new_name']:
                changes.append('name_change')

        if pd.notna(row['old_canton']) and pd.notna(row['new_canton']):
            if row['old_canton'] != row['new_canton']:
                changes.append('canton_change')

        if pd.notna(row['old_district_number']) and pd.notna(row['new_district_number']):
            if str(row['old_district_number']) != str(row['new_district_number']):
                changes.append('district_change')

        return '|'.join(changes) if changes else 'unknown'

    df['mutation_type'] = df.apply(get_change_type, axis=1)

    # Identify mergers and splits
    merger_counts = df.groupby('new_bfs_number').size()
    merger_bfs = merger_counts[merger_counts > 1].index
    df['is_merger'] = df['new_bfs_number'].isin(merger_bfs)

    split_counts = df.groupby('old_bfs_number').size()
    split_bfs = split_counts[split_counts > 1].index
    df['is_split'] = df['old_bfs_number'].isin(split_bfs)

    df['is_rename'] = (df['old_bfs_number'] == df['new_bfs_number']) & (df['old_name'] != df['new_name'])
    df['is_reassignment'] = ((df['old_canton'] != df['new_canton']) |
                             (df['old_district_number'] != df['new_district_number']))

    # Log statistics
    logger.info(f"Change types identified:")
    logger.info(f"  Mergers: {df['is_merger'].sum()}")
    logger.info(f"  Splits: {df['is_split'].sum()}")
    logger.info(f"  Renames: {df['is_rename'].sum()}")
    logger.info(f"  Reassignments: {df['is_reassignment'].sum()}")

    # Log change type distribution
    change_type_counts = df['mutation_type'].value_counts()
    logger.debug("Change type distribution:")
    for change_type, count in change_type_counts.head(10).items():
        logger.debug(f"  {change_type}: {count}")

    return df

def create_database_table(conn, logger):
    """Create municipal_changes table in database"""
    logger.info("Creating municipal_changes table...")

    cursor = conn.cursor()

    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS municipal_changes")

    # Create new table
    create_sql = """
    CREATE TABLE municipal_changes (
        change_id INTEGER PRIMARY KEY AUTOINCREMENT,
        mutation_number TEXT,

        -- Old (predecessor) structure
        old_canton TEXT,
        old_district_number TEXT,
        old_bfs_number TEXT,
        old_name TEXT,

        -- New (successor) structure
        new_canton TEXT,
        new_district_number TEXT,
        new_bfs_number TEXT,
        new_name TEXT,

        -- Mutation metadata
        mutation_date TEXT,
        mutation_type TEXT,

        -- Change type flags
        is_merger BOOLEAN,
        is_split BOOLEAN,
        is_rename BOOLEAN,
        is_reassignment BOOLEAN,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    cursor.execute(create_sql)
    conn.commit()
    logger.info("Table created successfully")

def import_to_database(df, conn, logger):
    """Import data into database"""
    logger.info(f"Importing {len(df)} records into database...")

    cursor = conn.cursor()

    # Convert date to string format
    if pd.api.types.is_datetime64_any_dtype(df['mutation_date']):
        df['mutation_date'] = df['mutation_date'].dt.strftime('%Y%m%d')

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    insert_sql = """
    INSERT INTO municipal_changes (
        mutation_number,
        old_canton, old_district_number, old_bfs_number, old_name,
        new_canton, new_district_number, new_bfs_number, new_name,
        mutation_date, mutation_type,
        is_merger, is_split, is_rename, is_reassignment
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    success_count = 0
    error_count = 0

    for idx, row in df.iterrows():
        try:
            cursor.execute(insert_sql, (
                row['mutation_number'],
                row['old_canton'],
                row['old_district_number'],
                row['old_bfs_number'],
                row['old_name'],
                row['new_canton'],
                row['new_district_number'],
                row['new_bfs_number'],
                row['new_name'],
                row['mutation_date'],
                row['mutation_type'],
                row['is_merger'],
                row['is_split'],
                row['is_rename'],
                row['is_reassignment']
            ))
            success_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f"Error inserting row {idx}: {e}")

    conn.commit()

    logger.info(f"Import complete: {success_count} successful, {error_count} errors")
    return success_count, error_count

def create_indexes(conn, logger):
    """Create database indexes for performance"""
    logger.info("Creating indexes...")

    cursor = conn.cursor()

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_old_bfs ON municipal_changes(old_bfs_number)",
        "CREATE INDEX IF NOT EXISTS idx_new_bfs ON municipal_changes(new_bfs_number)",
        "CREATE INDEX IF NOT EXISTS idx_mutation_date ON municipal_changes(mutation_date)",
        "CREATE INDEX IF NOT EXISTS idx_old_canton ON municipal_changes(old_canton)",
        "CREATE INDEX IF NOT EXISTS idx_new_canton ON municipal_changes(new_canton)",
        "CREATE INDEX IF NOT EXISTS idx_merger ON municipal_changes(is_merger)",
        "CREATE INDEX IF NOT EXISTS idx_mutation_type ON municipal_changes(mutation_type)"
    ]

    for idx_sql in indexes:
        cursor.execute(idx_sql)

    conn.commit()
    logger.info(f"Created {len(indexes)} indexes")

def verify_import(conn, logger):
    """Verify the import and log statistics"""
    logger.info("Verifying import...")

    cursor = conn.cursor()

    # Get statistics
    stats_queries = [
        ("Total records", "SELECT COUNT(*) FROM municipal_changes"),
        ("Unique old municipalities", "SELECT COUNT(DISTINCT old_bfs_number) FROM municipal_changes WHERE old_bfs_number IS NOT NULL"),
        ("Unique new municipalities", "SELECT COUNT(DISTINCT new_bfs_number) FROM municipal_changes WHERE new_bfs_number IS NOT NULL"),
        ("Total mergers", "SELECT COUNT(*) FROM municipal_changes WHERE is_merger = 1"),
        ("Total splits", "SELECT COUNT(*) FROM municipal_changes WHERE is_split = 1"),
        ("Total renames", "SELECT COUNT(*) FROM municipal_changes WHERE is_rename = 1"),
        ("Total reassignments", "SELECT COUNT(*) FROM municipal_changes WHERE is_reassignment = 1"),
        ("Earliest mutation", "SELECT MIN(mutation_date) FROM municipal_changes WHERE mutation_date IS NOT NULL"),
        ("Latest mutation", "SELECT MAX(mutation_date) FROM municipal_changes WHERE mutation_date IS NOT NULL")
    ]

    logger.info("Database statistics:")
    for label, query in stats_queries:
        cursor.execute(query)
        result = cursor.fetchone()[0]
        logger.info(f"  {label}: {result}")

    # Sample data for verification
    cursor.execute("""
        SELECT mutation_number, old_name, old_bfs_number, new_name, new_bfs_number, mutation_date
        FROM municipal_changes
        LIMIT 5
    """)

    logger.debug("Sample records:")
    for row in cursor.fetchall():
        logger.debug(f"  {row}")

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Starting municipal changes import")
    logger.info("="*60)

    # File paths
    excel_path = Path('data/Mutierte_Gemeinden.xlsx')
    db_path = Path('data/swiss_votings.db')

    # Check if files exist
    if not excel_path.exists():
        logger.error(f"Excel file not found: {excel_path}")
        return 1

    if not db_path.exists():
        logger.warning(f"Database does not exist yet: {db_path}")
        logger.info("Database will be created by the voting data import script")

    try:
        # Read and process Excel
        df = read_excel_with_validation(excel_path, logger)
        df = analyze_changes(df, logger)

        # Connect to database
        logger.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # Create table and import data
        create_database_table(conn, logger)
        success_count, error_count = import_to_database(df, conn, logger)

        # Create indexes and verify
        create_indexes(conn, logger)
        verify_import(conn, logger)

        # Close connection
        conn.close()
        logger.info("Database connection closed")

        # Summary
        logger.info("="*60)
        logger.info("Import completed successfully!")
        logger.info(f"Records imported: {success_count}")
        if error_count > 0:
            logger.warning(f"Errors encountered: {error_count}")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())