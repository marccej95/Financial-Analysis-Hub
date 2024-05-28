import os
import intrinsic_functions as intfunc
from dotenv import load_dotenv

load_dotenv()

# Run everytime a new ticker is added

ticker_symbol = "INTC"  # Change for ticker symbol when adding new stocks

conn = intfunc.connect_db_pagila(os.getenv("DB_USER"), os.getenv("DB_PASS"))

ticker_data = intfunc.get_full_ticker_data(ticker_symbol)
full_data_df = intfunc.create_ticker_dataframe(ticker_data)
intfunc.create_ticker_table(conn, ticker_symbol)
intfunc.insert_values(full_data_df, conn, "intrinsic", ticker_symbol)

intfunc.create_data_table(conn, "fundamental", ticker_symbol)
intfunc.create_data_table(conn, "income", ticker_symbol)
intfunc.create_data_table(conn, "balance", ticker_symbol)
intfunc.create_data_table(conn, "cashflow", ticker_symbol)

conn.close()
