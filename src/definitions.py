from pathlib import Path


MAIN_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = MAIN_DIR / 'data/processed'
INTERIM_DATA_DIR = MAIN_DIR / 'data/interim'
RAW_DATA_DIR = MAIN_DIR / 'data/raw'
REPORTS_DIR = MAIN_DIR / 'reports'
