from src.extract_flight_stats import extract_airports, extract_transtats, run_amadeus_extraction
from src.extract_reports import web_scrap_reports
from src.extract_stock_data import run_stock_extraction
from src.transform import transform_reports, transform_stocks, transform_airports, transform_transtats, transform_flights
from src.load import load_to_db, verify_data


def main():
    """Run the complete ETL pipeline"""
    print("🛫 Starting AeroInvest ETL Pipeline...")
    print("=" * 50)
    
    # Step 1: Extract data
    print("\n=== EXTRACTION ===")
    print("📥 Extracting data from sources...")
    
    airports = extract_airports()
    transtats = extract_transtats()
    flights = run_amadeus_extraction()

    reports_df, report_folder = web_scrap_reports()
    stock_data = run_stock_extraction()
    
    # Step 2: Transform data
    print("\n=== TRANSFORMATION ===")
    print("🔄 Cleaning and transforming data...")
    
    clean_airports_data = transform_airports(airports)
    clean_transtats = transform_transtats(transtats)
    clean_flights = transform_flights(flights)

    clean_reports_df = transform_reports(reports_df)
    clean_stock_data = transform_stocks(stock_data)  
    
    # Step 3: Load data
    print("\n=== LOADING ===")
    print("💾 Loading data to database...")
    
    load_to_db(clean_airports_data, "airports")
    load_to_db(clean_transtats, "air_traffic_statistics")
    load_to_db(clean_flights, "flights")

    load_to_db(clean_reports_df, "incident_accident_reports")
    load_to_db(clean_stock_data, "stocks")
    
    # Step 4: Verify everything worked
    print("\n=== VERIFICATION ===")
    print("✅ Verifying data was loaded correctly...")
    
    verify_data()

    print(f"All NTSB reports were downloaded to the {report_folder} folder.")
    
    print("\n🎉 ETL Pipeline completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()