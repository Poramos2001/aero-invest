import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf
import finnhub
import json


# Load environment variables
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Company List
with open("companies.json", "r") as f:
    companies = json.load(f)


def _extract_yahoo():
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


def _extract_finnhub():
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


def _missing_companies(companies, df):
    """Find the companies in `companies` that are not in the dataframe"""
    if not isinstance(companies, set):
        dict_tuples = set(companies.items())
    else:
        dict_tuples = companies
    df_tuples = set(zip(df["name"], df["symbol"]))

    # Find tuples in dict but not in DataFrame
    missing_tuples_set = dict_tuples - df_tuples
    return missing_tuples_set


def run_stock_extraction():
    """Run Yahoo/Finnhub stock extraction."""
    df = _extract_yahoo()

    missing_companies_set = _missing_companies(companies, df)
    num_not_found = len(missing_companies_set)

    if num_not_found != 0:
        print(f"\033[93mYahoo failed to extract {num_not_found} companies.\033[0m")
        print("\033[91mSwitching to Finnhub.\033[0m")
        df2 = _extract_finnhub()

        # Filter df2 to only include rows in missing_companies_set
        filtered_df2 = df2[df2.apply(lambda row: (row["name"], row["symbol"]) 
                                     in missing_companies_set, axis=1)]

        # Check if all stocks are now in the df
        missing_companies_set = _missing_companies(missing_companies_set, df2)
        num_not_found = len(missing_companies_set)

        if num_not_found != 0:
            print(f"\033[91m[ERROR] Extraction still failed for {num_not_found} stocks.\033[0m")
            print("The affected stocks are:")
            for name, symbol in missing_companies_set:
                print(f"\t{name} ({symbol})")
        else:
            print("\033[92mSuccessfully extracted the remaining companies.\033[0m")

        df = pd.concat([df, filtered_df2], ignore_index=True)
        return df

    print("\033[92mSuccessfully extracted all the companies.\033[0m")
    return df


if __name__ == "__main__":
    run_stock_extraction()
