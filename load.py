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



def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries.
    Handles special characters in column names and avoids immutabledict errors.
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Count records in stocks
        stocks_count = pd.read_sql("SELECT COUNT(*) AS count FROM stocks", engine)
        print(f"📊 Stocks in database: {stocks_count.iloc[0]['count']}")
        
        # Count records in airports
        airports_count = pd.read_sql("SELECT COUNT(*) AS count FROM airports", engine)
        print(f"📊 Airports in database: {airports_count.iloc[0]['count']}")
        
        # Show sample stock data
        df_stocks = pd.read_sql("SELECT * FROM stocks LIMIT 5", engine)
        print("\n📋 Sample stocks:")
        print('\tNOTE: Some columns may contain special characters or % signs.')
        print(df_stocks.head().to_string(index=False))
        
        # Show sample airport data
        df_airports = pd.read_sql("SELECT * FROM airports LIMIT 5", engine)
        print("\n📋 Sample airports:")
        print('\tNOTE: Some columns may contain spaces or special characters.')
        print(df_airports.head().to_string(index=False))
        
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
