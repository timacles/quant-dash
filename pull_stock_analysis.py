#!/usr/bin/env python3

import argparse
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
import yaml


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_config(path="config.yaml"):
    logging.info("Loading config from %s", path)
    with open(path, "r") as config_file:
        return yaml.safe_load(config_file)


def load_api_key(config):
    return config["api_key"]


def get_start_date(days_back=255):
    return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00")


def is_rate_limit_error(response_data):
    return (
        response_data.get("status") == "error"
        and response_data.get("code") == 429
        and "run out of api credits for the current minute"
        in str(response_data.get("message", "")).lower()
    )


def fetch_symbol_data(symbol, api_key, start_date):
    logging.info("Requesting API data for %s", symbol)
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "start_date": start_date,
        "timezone": "America/New_York",
        "apikey": api_key,
    }

    while True:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") == "ok":
            logging.info("API response OK for %s", symbol)
            return payload

        if is_rate_limit_error(payload):
            logging.warning("Rate limit reached for %s; waiting 60 seconds before retry", symbol)
            time.sleep(60)
            continue

        raise RuntimeError(f"API error for {symbol}: {payload}")


def payload_to_dataframe(payload):
    logging.info("Converting payload to dataframe")
    df = pd.DataFrame(payload["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    return df


def open_database():
    logging.info("Opening Postgres database financials")
    return psycopg2.connect(dbname="financials")


def ensure_stock_analysis_table(conn, sql_path="sql/stock_analysis.sql"):
    logging.info("Ensuring stock_analysis table exists using %s", sql_path)
    with open(sql_path, "r") as sql_file:
        ddl = sql_file.read()
    with conn.cursor() as cursor:
        cursor.execute(ddl)
    conn.commit()


def insert_symbol_rows(conn, symbol, df):
    logging.info("Upserting %s rows for %s into stock_analysis", len(df), symbol)
    rows = [
        (
            symbol,
            row.datetime.date(),
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume,
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO stock_analysis (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, symbol)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """,
            rows,
        )
    conn.commit()


def calculate_symbol_metrics(symbol, df):
    logging.info("Calculating 5-day metrics for %s", symbol)
    if len(df) < 5:
        raise RuntimeError(f"Not enough data returned for {symbol}")

    last5 = df.tail(5)
    ret_5d = (last5["close"].iloc[-1] / last5["close"].iloc[0] - 1) * 100
    vol_5d = last5["volume"].sum()

    return {
        "Symbol": symbol,
        "5D Return %": round(ret_5d, 2),
        "5D Volume": int(vol_5d),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Pull stock data for symbols and store OHLCV data."
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Comma-separated symbol list (e.g. AAPL,MSFT).",
    )
    parser.add_argument(
        "--print-data",
        action="store_true",
        help="Print raw API data after extraction.",
    )
    return parser.parse_args()


def parse_symbols(raw_symbols):
    symbols = [symbol.strip().upper() for symbol in raw_symbols.split(",")]
    return [symbol for symbol in symbols if symbol]


def process_symbol(conn, symbol, api_key, start_date, print_data=False):
    payload = fetch_symbol_data(symbol, api_key, start_date)
    df = payload_to_dataframe(payload)
    logging.info("Fetched dataframe for %s with %s rows", symbol, len(df))
    if print_data:
        print(df.to_string(index=False))
    insert_symbol_rows(conn, symbol, df)
    return calculate_symbol_metrics(symbol, df)


def main():
    configure_logging()
    logging.info("Starting stock analysis pull")

    args = parse_args()
    config = load_config()
    api_key = load_api_key(config)
    start_date = get_start_date(days_back=255)
    symbols = parse_symbols(args.symbol)
    if not symbols:
        raise RuntimeError("No valid symbols provided.")

    conn = open_database()
    try:
        ensure_stock_analysis_table(conn)

        results = []
        for symbol in symbols:
            logging.info("Processing symbol %s", symbol)
            result = process_symbol(
                conn, symbol, api_key, start_date, print_data=args.print_data
            )
            results.append(result)

        df_results = pd.DataFrame(results)
        logging.info("Completed processing all symbols")
        print(df_results.sort_values("5D Return %", ascending=False))
    finally:
        logging.info("Closing database connection")
        conn.close()


if __name__ == "__main__":
    main()
