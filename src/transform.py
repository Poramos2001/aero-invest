import pandas as pd
import os
from datetime import datetime


def transform_reports(df):
    df = df.replace("N/A", None)
    return df


def transform_stocks(df):
    """📊 Clean and enrich stock data"""
    if df.empty:
        print("⚠️ Stock data not found.")
        return pd.DataFrame()

    print(f"🔍 Transforming {len(df)} stock records...")

    # Normalize columns
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

    # Convert percentages and numeric fields
    for col in ["change_%", "52_wk_change_%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Categorize valuation
    if "p/e_ratio_(ttm)" in df.columns:
        df["p/e_ratio_(ttm)"] = pd.to_numeric(df["p/e_ratio_(ttm)"], errors="coerce")
        df["valuation_category"] = pd.cut(
            df["p/e_ratio_(ttm)"].fillna(-1),
            bins=[-1, 0, 15, 25, 40, 1000],
            labels=["N/A", "Undervalued", "Fair", "High", "Speculative"]
    )

    df["transformed_at"] = datetime.utcnow()

    return df


def transform_airports(df):
    """🛫 Filter and clean airport data"""
    if df.empty:
        print("⚠️ Airport data not found.")
        return pd.DataFrame()

    print(f"🔍 Transforming {len(df)} airports...")

    # Keep only large and medium airports
    if "type" in df.columns:
        df = df[df["type"].isin(["large_airport", "medium_airport"])]

    # Standardize names and countries
    df["iso_country"] = df["iso_country"].str.upper()
    df["region"] = df["iso_region"].str.split("-").str[1]

    df["transformed_at"] = datetime.utcnow()

    print(f"Transformed all airports")
    return df


def transform_transtats(df):
    """📈 Process TranStats passenger data"""
    if df.empty:
        print("⚠️ TranStats data not found.")
        return pd.DataFrame()

    print(f"🔍 Transforming {len(df)} TranStats records...")

    # Clean year/month
    df = df[df["Month"] != "TOTAL"]
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Month"] = pd.to_numeric(df["Month"], errors="coerce")

    # Calculate total passengers per year
    yearly = df.groupby("Year")[["TOTAL"]].sum().reset_index()
    yearly["YoY_Change_%"] = yearly["TOTAL"].pct_change() * 100

    yearly["transformed_at"] = datetime.utcnow()
    
    return yearly


def transform_flights(df):
    """✈️ Clean Amadeus flight data"""
    if df.empty:
        print("⚠️ No flight data found.")
        return pd.DataFrame()

    print(f"🔍 Transforming {len(df)} flight records...")

    # Convert datetimes
    for col in ["departure", "arrival"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Compute duration in hours
    df["duration_hours"] = (df["arrival"] - df["departure"]).dt.total_seconds() / 3600

    # Price as float
    df["price_EUR"] = pd.to_numeric(df["price_EUR"], errors="coerce")

    # Average price per airline
    price_summary = (
        df.groupby("carrier_code")["price_EUR"]
        .mean()
        .reset_index()
        .rename(columns={"price_EUR": "avg_price_EUR"})
    )

    df = df.merge(price_summary, on="carrier_code", how="left")
    df["transformed_at"] = datetime.utcnow()

    return df


if __name__ == "__main__":
    pass
