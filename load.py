"""
Data Loading Module for AeroInvest
"""

import pandas as pd
from sqlalchemy import create_engine
import json
import os
from datetime import datetime
# Load database config
with open('config.json', 'r') as f:
    DATABASE_CONFIG = json.load(f)

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@" \
           f"{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def load_to_db(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    💾 Load a DataFrame into PostgreSQL database
    
    Args:
        df (pd.DataFrame): Data to load
        table_name (str): Target table name
        if_exists (str): Behavior if table exists: 'replace', 'append', 'fail'
    """
    if df.empty:
        print(f"⚠️ No data to load for table '{table_name}'. Skipping.")
        return

    

    try:
        engine = create_engine(get_connection_string())
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"✅ Loaded {len(df)} records into table '{table_name}'")
    except Exception as e:
        print(f"⚠️ Failed to load table '{table_name}': {e}")
    finally:
        engine.dispose()

# -----------------------------
# Load all transformed CSVs
# -----------------------------
TRANSFORMED_DIR = "data/transformed"

def run_all_loads():
    """Load all transformed datasets into PostgreSQL"""
    print("\n🚀 Starting Data Loading Pipeline...\n")
    
    datasets = {
        "stocks": "stocks_clean.csv",
        "airports": "airports_clean.csv",
    }

    for table_name, file_name in datasets.items():
        path = os.path.join(TRANSFORMED_DIR, file_name)
        if os.path.exists(path):
            df = pd.read_csv(path)
            load_to_db(df, table_name)
        else:
            print(f"⚠️ File {file_name} not found. Skipping table '{table_name}'.")

    print("\n✅ All datasets loaded into PostgreSQL!\n")

def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Count records in stocks
        stocks_count = pd.read_sql("SELECT COUNT(*) as count FROM stocks", engine)
        print(f"📊 Stocks in database: {stocks_count.iloc[0]['count']}")
        
        # Count records in airports
        airports_count = pd.read_sql("SELECT COUNT(*) as count FROM airports", engine)
        print(f"📊 Airports in database: {airports_count.iloc[0]['count']}")
        
        # Show sample stock data
        sample_stocks = pd.read_sql("SELECT symbol, name, previous_open, previous_close, daily_pct_change FROM stocks LIMIT 5", engine)
        print("\n📋 Sample stocks:")
        print('\tNOTE: This is a non-exhaustive view of the table; some columns have been omitted for clarity.')
        print(sample_stocks.to_string(index=False))
        
        # Show sample airport data
        sample_airports = pd.read_sql("SELECT name, iso_country, municipality FROM airports LIMIT 5", engine)
        print("\n📋 Sample airports:")
        print('\tNOTE: This is a non-exhaustive view of the table; some columns have been omitted for clarity.')
        print(sample_airports.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
    finally:
        engine.dispose()
# -----------------------------
# Run as script
# -----------------------------
if __name__ == "__main__":
    run_all_loads()
    verify_data()