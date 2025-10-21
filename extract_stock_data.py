"""
Data Extraction Module
"""

import pandas as pd
import requests
import time
import os
import yfinance as yf
import finnhub

companies = {
        "Boeing": "BA",
        "Airbus": "AIR.PA",
        "Airbus ADR": "EADSY",
        "Lockheed Martin": "LMT",
        "Northrop Grumman": "NOC",
        "Raytheon Technologies": "RTX",
        "General Dynamics": "GD",
        "Textron": "TXT",
        "Spirit AeroSystems": "SPR",
        "Rolls-Royce": "RR.L",
        "Safran": "SAF.PA",
        "Leonardo": "LDO.MI",
        "Honeywell": "HON",
        "Embraer": "ERJ",
        "Virgin Galactic": "SPCE",
        "Rocket Lab": "RKLB",
        "L3Harris Technologies": "LHX",
    }



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
                    "52 Wk Range": f"{info.get('fiftyTwoWeekLow', '--')} - {info.get('fiftyTwoWeekHigh', '--')}"
                } 
            data_list.append(row)
        except Exception as e:
            print(f"⚠️ Yahoo fetch failed for {name} ({ticker}): {e}")
    df=pd.DataFrame(data_list)
    print(df)
    return df





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
                "52 Wk Range": "--"          # Not available on free plan
            }
            data_list.append(row)
        except Exception as e:
            print(f"⚠️ Error fetching {name} ({symbol}): {e}")

    df = pd.DataFrame(data_list)
    print(df)
    return df

    

def fetch_stock_data():
    """
    Wrapper function: tries Yahoo first, falls back to Finnhub for missing data.
    """
    try:
        df = extract_yahoo()
        if df.empty:
            print("⚠️ Yahoo failed, falling back to Finnhub")
            df = extract_finhub()
    except Exception as e:
        print(f"⚠️ Yahoo failed: {e}. Falling back to Finnhub")
        df = extract_finhub()
        if df.empty:
            print("⚠️ Finhub failed, out of sources")
    return df


if __name__ == "__main__":
    # Only run when executing this file directly
    df = fetch_stock_data()
    print(df)

fetch_stock_data()