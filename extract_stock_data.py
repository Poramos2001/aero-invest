"""
Data Extraction Module
"""

import pandas as pd
import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
import finnhub

# Load environment variables

load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ICAO_API_KEY = os.getenv("ICAO_API_KEY")
AMADEUS_API_KEY = os.getenv("AMADEUS_API_KEY")
AMADEUS_API_SECRET = os.getenv("AMADEUS_API_SECRET")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Company List
companies = {
     # Aerospace & Defense
    "Boeing": "BA",
    "Airbus SE": "AIR.PA",
    "Airbus ADR": "EADSY",
    "Lockheed Martin": "LMT",
    "Northrop Grumman": "NOC",
    "Raytheon Technologies (RTX Corp)": "RTX",
    "General Dynamics": "GD",
    "Textron": "TXT",
    "Spirit AeroSystems": "SPR",
    "Rolls-Royce Holdings": "RR.L",
    "Safran": "SAF.PA",
    "Leonardo S.p.A.": "LDO.MI",
    "Honeywell International": "HON",
    "Embraer S.A.": "ERJ",
    "Virgin Galactic Holdings": "SPCE",
    "Rocket Lab USA": "RKLB",
    "L3Harris Technologies": "LHX",

    # Air Carriers
    "Delta Air Lines": "DAL",
    "United Airlines Holdings": "UAL",
    "Southwest Airlines": "LUV",
    "American Airlines Group": "AAL",
    "Alaska Air Group": "ALK",

    # Private Space Company (no public ticker)
    "SpaceX (Private)": "SPAX.PVT"
    }


# Yahoo Finance Extractor
def extract_yahoo():
    """
    Extract current stock information from Yahoo Finance
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    

    data_list = []
    print("📈 Fetching live flight data from API...")
    for name, ticker in companies.items():
        try:
            tk=yf.Ticker(ticker)
            info = tk.info
            row = {
                    "Symbol": info.get("symbol", "--"),
                    "Name": name,
                    "Price": info.get("regularMarketPrice", "--"),
                    "Change": info.get("regularMarketChange", "--"),
                    "Change %": info.get("regularMarketChangePercent", "--"),
                    "Volume": info.get("volume", "--"),
                    "Avg Vol (3M)": info.get("averageVolume", "--"),
                    "Market Cap": info.get("marketCap", "--"),
                    "P/E Ratio (TTM)": info.get("trailingPE", "--"),
                    "52 Wk Change %": info.get("52WeekChange", "--"),
                    "52 Wk Range": f"{info.get('fiftyTwoWeekLow', '--')} - {info.get('fiftyTwoWeekHigh', '--')}",
                    "Fetched At": datetime.utcnow()
                } 
            data_list.append(row)
            print(f"{name}({ticker}) fetched successfully.")
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Yahoo fetch failed for {name} ({ticker}): {e}")
    df=pd.DataFrame(data_list)
    print(df)
    return df

# Finnhub Extractor

def extract_finhub():
    finnhub_client = finnhub.Client(api_key="d3rl529r01qopgh8udh0d3rl529r01qopgh8udhg")


    data_list = []

    print("📈 Fetching live stock data from Finnhub API (free plan)...")

    for name, symbol in companies.items():
        try:
            quote = finnhub_client.quote(symbol)
            row = {
                "Symbol": symbol,
                "Name": name,
                "Price": quote.get("c", "--"),
                "Change": quote.get("c", 0) - quote.get("pc", 0),
                "Change %": ((quote.get("c", 0) - quote.get("pc", 0)) / quote.get("pc", 1)) * 100 if quote.get("pc") else "--",
                "Volume": "--", # Not available on free plan
                "Avg Vol (3M)": "--",       # Not available on free plan
                "Market Cap": "--",          # Not available on free plan
                "P/E Ratio (TTM)": "--",    # Not available on free plan
                "52 Wk Change %": "--",      # Not available on free plan
                "52 Wk Range": "--",          # Not available on free plan
                "Fetched At": datetime.utcnow()

            }
            data_list.append(row)
            print(f"{name}({symbol}) fetched successfully.")
            time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Error fetching {name} ({symbol}): {e}")

    df = pd.DataFrame(data_list)
    print(df)

    return df

# ICAO Data Extractors
ICAO_BASE_URL = "https://applications.icao.int/dataservices/api"

def fetch_icao_dataset(dataset_name, extra_params=None):
    params = {"api_key": ICAO_API_KEY, "format": "JSON"}
    if extra_params:
        params.update(extra_params)
    url = f"{ICAO_BASE_URL}/{dataset_name}"
    print(f"🌐 Fetching ICAO dataset: {dataset_name} ...")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data)
        df["Fetched At"] = datetime.utcnow()
        print(f"✅ ICAO {dataset_name} fetched successfully ({len(df)} records).")
        return df
    except Exception as e:
        print(f"⚠️ ICAO {dataset_name} fetch failed: {e}")
        return pd.DataFrame()

def extract_notams():
    return fetch_icao_dataset("notams-realtime-list")

def extract_accidents():
    return fetch_icao_dataset("accidents")

def extract_incidents():
    return fetch_icao_dataset("incidents")

# Amadeus Flight Availability extractor
AMADEUS_TOKEN_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
AMADEUS_FLIGHTS_URL = "https://test.api.amadeus.com/v1/shopping/availability/flight-availabilities"

def get_amadeus_token():
    """Authenticate and get Amadeus API access token."""
    try:
        response = requests.post(AMADEUS_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": AMADEUS_API_KEY,
            "client_secret": AMADEUS_API_SECRET
        })
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"⚠️ Failed to get Amadeus token: {e}")
        return None

def extract_flight_availabilities(origin, destination, departure_date):
    """Extract flight seat availability for a given route/date."""
    token = get_amadeus_token()
    if not token:
        return pd.DataFrame()

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": 1
    }
    try:
        response = requests.get(AMADEUS_FLIGHTS_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        flights = []
        for item in data.get("data", []):
            flight = item.get("flightDesignator", {})
            for segment in item.get("availability", []):
                flights.append({
                    "origin": origin,
                    "destination": destination,
                    "date": departure_date,
                    "flight_number": flight,
                    "cabin": segment.get("cabin"),
                    "available_seats": segment.get("numberOfBookableSeats"),
                    "Fetched At": datetime.utcnow()
                })
        print(f"✅ Amadeus flights fetched successfully ({len(flights)} records).")
        return pd.DataFrame(flights)
    except Exception as e:
        print(f"⚠️ Amadeus flight fetch failed: {e}")
        return pd.DataFrame()

# Master Wrapper Function
def run_all_extractions():
    """Runs all extractors and saves outputs to /data."""
    # --- Stocks ---
    try:
        df_stocks = extract_yahoo()
        if df_stocks.empty:
            print("⚠️ Yahoo returned no data, trying Finnhub...")
            df_stocks = extract_finhub()
        df_stocks.to_csv(os.path.join(DATA_DIR, "raw_stocks.csv"), index=False)
        print("💾 Stocks saved: data/raw_stocks.csv")
    except Exception as e:
        print(f"❌ Stock extraction failed: {e}")

    # --- ICAO Datasets ---
    for name, func in {
        "notams": extract_notams,
        "accidents": extract_accidents,
        "incidents": extract_incidents
    }.items():
        try:
            df = func()
            if not df.empty:
                df.to_csv(os.path.join(DATA_DIR, f"{name}.csv"), index=False)
                print(f"💾 ICAO {name} saved.")
        except Exception as e:
            print(f"⚠️ Failed to fetch ICAO {name}: {e}")

    # --- Amadeus Flight Availability ---
    df_flights = extract_flight_availabilities("CDG", "JFK", "2025-10-25")
    if not df_flights.empty:
        df_flights.to_csv(os.path.join(DATA_DIR, "flight_availabilities.csv"), index=False)
        print("💾 Flight availability data saved.")

    print("\n✅ Extraction complete for all data sources.")

# Main Execution
if __name__ == "__main__":
    run_all_extractions()
    