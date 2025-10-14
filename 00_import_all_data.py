#!/usr/bin/env python3
"""
Complete Swiss Voting Data Import Pipeline
==========================================
This script imports both:
1. Municipal changes from Excel (Gemeindefusionen)
2. Voting data from JSON files

Creates a SQLite database with complete voting history and municipal tracking.
"""

import pandas as pd
import sqlite3
import json
from pathlib import Path
from datetime import datetime
import logging
import sys
from tqdm import tqdm

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = Path('data/swiss_votings.db')
EXCEL_PATH = Path('data/Mutierte_Gemeinden.xlsx')
VOTES_DIR = Path('data/votes')
LOG_DIR = Path('logs')

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Setup comprehensive logging"""
    LOG_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOG_DIR / f'import_all_{timestamp}.log'

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Setup logger
    logger = logging.getLogger('SwissVoting')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# ============================================================================
# DATABASE SETUP
# ============================================================================

def create_database(logger):
    """Create database with all necessary tables"""
    logger.info("Creating database structure...")

    # Remove existing database
    if DB_PATH.exists():
        DB_PATH.unlink()
        logger.info(f"Removed existing database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")

    # Create tables
    tables_sql = """
    -- Votings table
    CREATE TABLE votings (
        voting_id INTEGER PRIMARY KEY AUTOINCREMENT,
        voting_date TEXT NOT NULL UNIQUE,
        timestamp TEXT,
        source_file TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Proposals table
    CREATE TABLE proposals (
        proposal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        voting_id INTEGER NOT NULL,
        vorlage_id INTEGER,
        title_de TEXT,
        title_fr TEXT,
        title_it TEXT,
        title_rm TEXT,
        title_en TEXT,
        proposal_type INTEGER,
        angenommen BOOLEAN,
        doppeltes_mehr BOOLEAN,
        FOREIGN KEY (voting_id) REFERENCES votings(voting_id)
    );

    -- Cantons table
    CREATE TABLE cantons (
        canton_id TEXT PRIMARY KEY,
        canton_name TEXT NOT NULL,
        first_seen_date TEXT,
        last_seen_date TEXT
    );

    -- Districts table
    CREATE TABLE districts (
        district_id TEXT PRIMARY KEY,
        district_name TEXT NOT NULL,
        canton_id TEXT,
        first_seen_date TEXT,
        last_seen_date TEXT,
        FOREIGN KEY (canton_id) REFERENCES cantons(canton_id)
    );

    -- Municipalities table
    CREATE TABLE municipalities (
        municipality_id TEXT PRIMARY KEY,
        municipality_name TEXT NOT NULL,
        district_id TEXT,
        canton_id TEXT,
        parent_id TEXT,
        first_seen_date TEXT,
        last_seen_date TEXT,
        successor_id TEXT,
        FOREIGN KEY (district_id) REFERENCES districts(district_id),
        FOREIGN KEY (canton_id) REFERENCES cantons(canton_id)
    );

    -- Voting results table
    CREATE TABLE voting_results (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        voting_id INTEGER NOT NULL,
        proposal_id INTEGER NOT NULL,
        geo_level TEXT NOT NULL,
        geo_id TEXT NOT NULL,
        geo_name TEXT,
        ja_stimmen_absolut INTEGER,
        nein_stimmen_absolut INTEGER,
        ja_stimmen_prozent REAL,
        stimmbeteiligung_prozent REAL,
        gueltige_stimmen INTEGER,
        eingelegte_stimmzettel INTEGER,
        anzahl_stimmberechtigte INTEGER,
        gebiet_ausgezaehlt BOOLEAN,
        FOREIGN KEY (voting_id) REFERENCES votings(voting_id),
        FOREIGN KEY (proposal_id) REFERENCES proposals(proposal_id)
    );

    -- Spatial references table
    CREATE TABLE spatial_references (
        reference_id INTEGER PRIMARY KEY AUTOINCREMENT,
        voting_id INTEGER NOT NULL,
        spatial_unit TEXT NOT NULL,
        spatial_date TEXT NOT NULL,
        FOREIGN KEY (voting_id) REFERENCES votings(voting_id)
    );

    -- Municipal changes table
    CREATE TABLE municipal_changes (
        change_id INTEGER PRIMARY KEY AUTOINCREMENT,
        mutation_number TEXT,
        old_canton TEXT,
        old_district_number TEXT,
        old_bfs_number TEXT,
        old_name TEXT,
        new_canton TEXT,
        new_district_number TEXT,
        new_bfs_number TEXT,
        new_name TEXT,
        mutation_date TEXT,
        mutation_type TEXT,
        is_merger BOOLEAN,
        is_split BOOLEAN,
        is_rename BOOLEAN,
        is_reassignment BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    for sql in tables_sql.split(';'):
        if sql.strip():
            cursor.execute(sql)

    conn.commit()
    logger.info("Database tables created")

    # Create indexes
    indexes = [
        "CREATE INDEX idx_votings_date ON votings(voting_date)",
        "CREATE INDEX idx_proposals_voting ON proposals(voting_id)",
        "CREATE INDEX idx_results_voting ON voting_results(voting_id)",
        "CREATE INDEX idx_results_proposal ON voting_results(proposal_id)",
        "CREATE INDEX idx_results_geo ON voting_results(geo_level, geo_id)",
        "CREATE INDEX idx_municipalities_dates ON municipalities(first_seen_date, last_seen_date)",
        "CREATE INDEX idx_old_bfs ON municipal_changes(old_bfs_number)",
        "CREATE INDEX idx_new_bfs ON municipal_changes(new_bfs_number)",
    ]

    for idx_sql in indexes:
        cursor.execute(idx_sql)

    conn.commit()
    logger.info(f"Created {len(indexes)} indexes")

    return conn

# ============================================================================
# MUNICIPAL CHANGES IMPORT
# ============================================================================

def import_municipal_changes(conn, logger):
    """Import municipal changes from Excel"""
    logger.info("="*60)
    logger.info("IMPORTING MUNICIPAL CHANGES")
    logger.info("="*60)

    if not EXCEL_PATH.exists():
        logger.error(f"Excel file not found: {EXCEL_PATH}")
        return False

    try:
        # Read Excel with proper header handling
        logger.info(f"Reading Excel: {EXCEL_PATH}")
        df = pd.read_excel(EXCEL_PATH, sheet_name='Daten', header=1)
        logger.info(f"Read {len(df)} rows from Excel")

        # Rename columns
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

        # Remove header row if present in data
        if len(df) > 0 and str(df.iloc[0]['mutation_number']).lower() == 'mutationsnummer':
            logger.warning("Removing header row from data")
            df = df[df['mutation_number'] != 'Mutationsnummer'].reset_index(drop=True)

        # Analyze changes
        logger.info("Analyzing change types...")

        # Identify mergers
        merger_counts = df.groupby('new_bfs_number').size()
        merger_bfs = merger_counts[merger_counts > 1].index
        df['is_merger'] = df['new_bfs_number'].isin(merger_bfs)

        # Identify splits
        split_counts = df.groupby('old_bfs_number').size()
        split_bfs = split_counts[split_counts > 1].index
        df['is_split'] = df['old_bfs_number'].isin(split_bfs)

        # Identify renames and reassignments
        df['is_rename'] = (df['old_bfs_number'] == df['new_bfs_number']) & (df['old_name'] != df['new_name'])
        df['is_reassignment'] = ((df['old_canton'] != df['new_canton']) |
                                 (df['old_district_number'] != df['new_district_number']))

        # Create mutation type
        def get_mutation_type(row):
            types = []
            if row['is_merger']: types.append('merger')
            if row['is_split']: types.append('split')
            if row['is_rename']: types.append('rename')
            if row['is_reassignment']: types.append('reassignment')
            return '|'.join(types) if types else 'other'

        df['mutation_type'] = df.apply(get_mutation_type, axis=1)

        logger.info(f"Mergers: {df['is_merger'].sum()}")
        logger.info(f"Splits: {df['is_split'].sum()}")
        logger.info(f"Renames: {df['is_rename'].sum()}")
        logger.info(f"Reassignments: {df['is_reassignment'].sum()}")

        # Convert date format
        if pd.api.types.is_datetime64_any_dtype(df['mutation_date']):
            df['mutation_date'] = df['mutation_date'].dt.strftime('%Y%m%d')

        # Insert into database
        cursor = conn.cursor()
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
        for idx, row in df.iterrows():
            try:
                cursor.execute(insert_sql, tuple(row[col] for col in [
                    'mutation_number', 'old_canton', 'old_district_number', 'old_bfs_number', 'old_name',
                    'new_canton', 'new_district_number', 'new_bfs_number', 'new_name',
                    'mutation_date', 'mutation_type', 'is_merger', 'is_split', 'is_rename', 'is_reassignment'
                ]))
                success_count += 1
            except Exception as e:
                logger.error(f"Error inserting row {idx}: {e}")

        conn.commit()
        logger.info(f"Imported {success_count}/{len(df)} municipal changes")

        return True

    except Exception as e:
        logger.error(f"Error importing municipal changes: {e}", exc_info=True)
        return False

# ============================================================================
# VOTING DATA IMPORT
# ============================================================================

def process_voting_file(file_path, conn, logger):
    """Process a single voting JSON file"""
    cursor = conn.cursor()

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        voting_date = data.get('abstimmtag', '')
        timestamp = data.get('timestamp', '')

        # Insert voting record
        cursor.execute("""
            INSERT OR IGNORE INTO votings (voting_date, timestamp, source_file)
            VALUES (?, ?, ?)
        """, (voting_date, timestamp, file_path.name))

        voting_id = cursor.lastrowid
        if voting_id == 0:
            cursor.execute("SELECT voting_id FROM votings WHERE voting_date = ?", (voting_date,))
            voting_id = cursor.fetchone()[0]

        # Insert spatial references
        if 'spatial_reference' in data:
            for ref in data['spatial_reference']:
                cursor.execute("""
                    INSERT INTO spatial_references (voting_id, spatial_unit, spatial_date)
                    VALUES (?, ?, ?)
                """, (voting_id, ref.get('spatial_unit'), ref.get('spatial_date')))

        # Process proposals
        if 'schweiz' in data and 'vorlagen' in data['schweiz']:
            for vorlage in data['schweiz']['vorlagen']:
                # Extract titles
                titles = {}
                if 'vorlagenTitel' in vorlage:
                    for title in vorlage['vorlagenTitel']:
                        lang_key = title.get('langKey', '')
                        titles[f'title_{lang_key}'] = title.get('text', '')

                # Insert proposal
                cursor.execute("""
                    INSERT INTO proposals (
                        voting_id, vorlage_id, title_de, title_fr, title_it, title_rm, title_en,
                        proposal_type, angenommen, doppeltes_mehr
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    voting_id,
                    vorlage.get('vorlagenId'),
                    titles.get('title_de'),
                    titles.get('title_fr'),
                    titles.get('title_it'),
                    titles.get('title_rm'),
                    titles.get('title_en'),
                    vorlage.get('vorlagenArtId'),
                    vorlage.get('vorlageAngenommen'),
                    vorlage.get('doppeltesMehr')
                ))

                proposal_id = cursor.lastrowid

                # Switzerland-level results
                if 'resultat' in vorlage:
                    res = vorlage['resultat']
                    cursor.execute("""
                        INSERT INTO voting_results (
                            voting_id, proposal_id, geo_level, geo_id, geo_name,
                            ja_stimmen_absolut, nein_stimmen_absolut, ja_stimmen_prozent,
                            stimmbeteiligung_prozent, gueltige_stimmen, eingelegte_stimmzettel,
                            anzahl_stimmberechtigte, gebiet_ausgezaehlt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        voting_id, proposal_id, 'switzerland', '0', 'Schweiz',
                        res.get('jaStimmenAbsolut'), res.get('neinStimmenAbsolut'),
                        res.get('jaStimmenInProzent'), res.get('stimmbeteiligungInProzent'),
                        res.get('gueltigeStimmen'), res.get('eingelegteStimmzettel'),
                        res.get('anzahlStimmberechtigte'), res.get('gebietAusgezaehlt')
                    ))

                # Process cantons
                if 'kantone' in vorlage:
                    for kanton in vorlage['kantone']:
                        canton_id = kanton.get('geoLevelnummer')
                        canton_name = kanton.get('geoLevelname')

                        # Update canton
                        cursor.execute("""
                            INSERT OR IGNORE INTO cantons (canton_id, canton_name)
                            VALUES (?, ?)
                        """, (canton_id, canton_name))

                        cursor.execute("""
                            UPDATE cantons
                            SET first_seen_date = MIN(IFNULL(first_seen_date, ?), ?),
                                last_seen_date = MAX(IFNULL(last_seen_date, ?), ?)
                            WHERE canton_id = ?
                        """, (voting_date, voting_date, voting_date, voting_date, canton_id))

                        # Canton results
                        if 'resultat' in kanton:
                            res = kanton['resultat']
                            cursor.execute("""
                                INSERT INTO voting_results (
                                    voting_id, proposal_id, geo_level, geo_id, geo_name,
                                    ja_stimmen_absolut, nein_stimmen_absolut, ja_stimmen_prozent,
                                    stimmbeteiligung_prozent, gueltige_stimmen, eingelegte_stimmzettel,
                                    anzahl_stimmberechtigte, gebiet_ausgezaehlt
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                voting_id, proposal_id, 'canton', canton_id, canton_name,
                                res.get('jaStimmenAbsolut'), res.get('neinStimmenAbsolut'),
                                res.get('jaStimmenInProzent'), res.get('stimmbeteiligungInProzent'),
                                res.get('gueltigeStimmen'), res.get('eingelegteStimmzettel'),
                                res.get('anzahlStimmberechtigte'), res.get('gebietAusgezaehlt')
                            ))

                        # Process districts
                        if 'bezirke' in kanton:
                            for bezirk in kanton['bezirke']:
                                district_id = bezirk.get('geoLevelnummer')
                                district_name = bezirk.get('geoLevelname')

                                cursor.execute("""
                                    INSERT OR IGNORE INTO districts (district_id, district_name, canton_id)
                                    VALUES (?, ?, ?)
                                """, (district_id, district_name, canton_id))

                                cursor.execute("""
                                    UPDATE districts
                                    SET first_seen_date = MIN(IFNULL(first_seen_date, ?), ?),
                                        last_seen_date = MAX(IFNULL(last_seen_date, ?), ?)
                                    WHERE district_id = ?
                                """, (voting_date, voting_date, voting_date, voting_date, district_id))

                                # District results
                                if 'resultat' in bezirk:
                                    res = bezirk['resultat']
                                    cursor.execute("""
                                        INSERT INTO voting_results (
                                            voting_id, proposal_id, geo_level, geo_id, geo_name,
                                            ja_stimmen_absolut, nein_stimmen_absolut, ja_stimmen_prozent,
                                            stimmbeteiligung_prozent, gueltige_stimmen, eingelegte_stimmzettel,
                                            anzahl_stimmberechtigte, gebiet_ausgezaehlt
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        voting_id, proposal_id, 'district', district_id, district_name,
                                        res.get('jaStimmenAbsolut'), res.get('neinStimmenAbsolut'),
                                        res.get('jaStimmenInProzent'), res.get('stimmbeteiligungInProzent'),
                                        res.get('gueltigeStimmen'), res.get('eingelegteStimmzettel'),
                                        res.get('anzahlStimmberechtigte'), res.get('gebietAusgezaehlt')
                                    ))

                        # Process municipalities (UNDER CANTONS!)
                        if 'gemeinden' in kanton:
                            for gemeinde in kanton['gemeinden']:
                                municipality_id = gemeinde.get('geoLevelnummer')
                                municipality_name = gemeinde.get('geoLevelname')
                                parent_id = gemeinde.get('geoLevelParentnummer')

                                cursor.execute("""
                                    INSERT OR IGNORE INTO municipalities (
                                        municipality_id, municipality_name, parent_id, canton_id
                                    ) VALUES (?, ?, ?, ?)
                                """, (municipality_id, municipality_name, parent_id, canton_id))

                                cursor.execute("""
                                    UPDATE municipalities
                                    SET first_seen_date = MIN(IFNULL(first_seen_date, ?), ?),
                                        last_seen_date = MAX(IFNULL(last_seen_date, ?), ?)
                                    WHERE municipality_id = ?
                                """, (voting_date, voting_date, voting_date, voting_date, municipality_id))

                                # Municipality results
                                if 'resultat' in gemeinde:
                                    res = gemeinde['resultat']
                                    cursor.execute("""
                                        INSERT INTO voting_results (
                                            voting_id, proposal_id, geo_level, geo_id, geo_name,
                                            ja_stimmen_absolut, nein_stimmen_absolut, ja_stimmen_prozent,
                                            stimmbeteiligung_prozent, gueltige_stimmen, eingelegte_stimmzettel,
                                            anzahl_stimmberechtigte, gebiet_ausgezaehlt
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        voting_id, proposal_id, 'municipality', municipality_id, municipality_name,
                                        res.get('jaStimmenAbsolut'), res.get('neinStimmenAbsolut'),
                                        res.get('jaStimmenInProzent'), res.get('stimmbeteiligungInProzent'),
                                        res.get('gueltigeStimmen'), res.get('eingelegteStimmzettel'),
                                        res.get('anzahlStimmberechtigte'), res.get('gebietAusgezaehlt')
                                    ))

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing {file_path.name}: {e}")
        return False

def import_voting_data(conn, logger):
    """Import all voting data from JSON files"""
    logger.info("="*60)
    logger.info("IMPORTING VOTING DATA")
    logger.info("="*60)

    json_files = sorted(VOTES_DIR.glob('sd-t-17-02-*-eidgAbstimmung.json'))
    logger.info(f"Found {len(json_files)} JSON files to process")

    if not json_files:
        logger.error("No JSON files found!")
        return False

    success_count = 0
    error_count = 0

    for file_path in tqdm(json_files, desc="Processing voting files"):
        if process_voting_file(file_path, conn, logger):
            success_count += 1
        else:
            error_count += 1

    logger.info(f"Successfully processed: {success_count}/{len(json_files)} files")
    if error_count > 0:
        logger.warning(f"Errors encountered: {error_count} files")

    return True

# ============================================================================
# VERIFICATION
# ============================================================================

def verify_import(conn, logger):
    """Verify the complete import"""
    logger.info("="*60)
    logger.info("VERIFICATION")
    logger.info("="*60)

    cursor = conn.cursor()

    tables = [
        ('votings', 'Voting events'),
        ('proposals', 'Proposals'),
        ('cantons', 'Cantons'),
        ('districts', 'Districts'),
        ('municipalities', 'Municipalities'),
        ('voting_results', 'Voting results'),
        ('spatial_references', 'Spatial references'),
        ('municipal_changes', 'Municipal changes')
    ]

    logger.info("Database statistics:")
    for table, description in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        logger.info(f"  {description:25} {count:10,} records")

    # Check municipality results specifically
    cursor.execute("SELECT COUNT(*) FROM voting_results WHERE geo_level = 'municipality'")
    muni_results = cursor.fetchone()[0]
    logger.info(f"\nMunicipality-level results: {muni_results:,}")

    if muni_results == 0:
        logger.error("WARNING: No municipality results found! Check JSON structure.")

    # Sample municipalities
    cursor.execute("""
        SELECT municipality_id, municipality_name, canton_id, first_seen_date, last_seen_date
        FROM municipalities
        LIMIT 5
    """)

    municipalities = cursor.fetchall()
    if municipalities:
        logger.info("\nSample municipalities:")
        for mun in municipalities:
            logger.info(f"  {mun[0]}: {mun[1]} (Canton: {mun[2]}, {mun[3]} - {mun[4] or 'current'})")

    return True

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main function to run complete import"""
    logger = setup_logging()

    logger.info("="*60)
    logger.info("SWISS VOTING DATA IMPORT PIPELINE")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("="*60)

    try:
        # Create database
        conn = create_database(logger)

        # Import municipal changes
        if not import_municipal_changes(conn, logger):
            logger.error("Failed to import municipal changes")
            return 1

        # Import voting data
        if not import_voting_data(conn, logger):
            logger.error("Failed to import voting data")
            return 1

        # Verify import
        verify_import(conn, logger)

        # Close connection
        conn.close()

        logger.info("="*60)
        logger.info("IMPORT COMPLETED SUCCESSFULLY")
        logger.info(f"Database created at: {DB_PATH}")
        logger.info(f"Log file: {LOG_DIR}/import_all_*.log")
        logger.info("="*60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())