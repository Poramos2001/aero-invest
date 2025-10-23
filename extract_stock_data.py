"""
Data Extraction Module
"""

import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
import finnhub
import json


# Load environment variables
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ICAO_API_KEY = os.getenv("ICAO_API_KEY")
AMADEUS_API_KEY = os.getenv("AMADEUS_API_KEY")
AMADEUS_API_SECRET = os.getenv("AMADEUS_API_SECRET")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Company List
with open("companies.json", "r") as f:
    companies = json.load(f)


def extract_yahoo():
    """
    Extract current stock information from Yahoo Finance.
    
    Attention: the function requires the companies names and tickers to be in 
    a `companies` variable.
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    # ANSI coded for blue
    print("\n\n\033[94mFetching stock data from Yahoo Finance...\033[0m")

    data_list = []
    today = datetime.utcnow().replace(second=0, microsecond=0)

    for name, ticker in companies.items():
        try:
            tk=yf.Ticker(ticker)
            info = tk.info
            row = {
                "Symbol": info.get("symbol", None),
                "Name": name,
                "Previous Open": info.get("regularMarketOpen", None),
                "Previous Close": info.get("previousClose", None),
                "Daily % Change": info.get("regularMarketChangePercent", None),
                "Volume": info.get("volume", None),
                "Avg Volume (3M)": info.get("averageVolume", None),
                "Market Cap": info.get("marketCap", None),
                "P/E Ratio (TTM)": info.get("trailingPE", None),
                "EPS (TTM)": info.get("trailingEps", None),
                "52 Wk Change %": info.get("52WeekChange", None),
                "52 Wk Range": f"{info.get('fiftyTwoWeekLow', None)} - {info.get('fiftyTwoWeekHigh', None)}",
                "Dividend Yield": info.get("dividendYield", None),
                "Forward Dividend": info.get("dividendRate", None),
                "Next Earnings Date": info.get("earningsDate", None),
                "YTD Return": info.get("ytdReturn", None),
                "Fetched At": today
            }
            data_list.append(row)
            print(f"{name}({ticker}) fetched successfully.")

        except Exception as e:
            # \033[91m is the ANSI escape code for red
            print(f"\033[91m[ERROR] Yahoo fetch failed for {name} ({ticker}): {e}\033[0m")
    
    df=pd.DataFrame(data_list)
    print(df)
    return df


def extract_finnhub():
    """
    Extract current stock information from Finnhub.
    
    Attention: the function requires the companies names and tickers to be in 
    a `companies` variable.
    
    Returns:
        pandas.DataFrame: Company stock information for the selected aerospace companies
    """
    finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

    # ANSI coded for blue
    print("\n\n\033[94mFetching stock data from Finnhub API (free plan)...\033[0m")

    data_list = []
    today = datetime.utcnow().replace(second=0, microsecond=0)

    for name, symbol in companies.items():
        try:
            quote = finnhub_client.quote(symbol)
            profile = finnhub_client.company_profile2(symbol=symbol)
            metrics = finnhub_client.company_basic_financials(symbol, 'all')

            row = {
                "Symbol": symbol,
                "Name": name,
                "Previous Open": quote.get("o", None),
                "Previous Close": quote.get("pc", None),
                "Daily % Change": quote.get("dp", None),
                "Volume": quote.get("v", None),
                "Avg Volume (3M)": None,  # Not available in free plan
                "Market Cap": profile.get("marketCapitalization", None),
                "P/E Ratio (TTM)": metrics.get("metric", {}).get("peTTM", None),
                "EPS (TTM)": metrics.get("metric", {}).get("epsTTM", None),
                "52 Wk Change %": None,  # Not available in free plan
                "52 Wk Range": None,     # Not available in free plan
                "Dividend Yield": metrics.get("metric", {}).get("dividendYieldIndicatedAnnual", None),
                "Forward Dividend": None,  # Not available in free plan
                "Next Earnings Date": metrics.get("metric", {}).get("nextEarningsDate", None),
                "YTD Return": None,  # Not available in free plan
                "Fetched At": today
            }
            data_list.append(row)
            print(f"{name}({symbol}) fetched successfully.")

        except Exception as e:
            # \033[91m is the ANSI escape code for red
            print(f"\033[91m[ERROR] Finnhub fetch failed for {name} ({symbol}): {e}\033[0m")

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
            df_stocks = extract_finnhub()
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
    extract_yahoo()
    