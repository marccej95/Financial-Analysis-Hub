import os
import intrinsic_functions as intfunc
from dotenv import load_dotenv

load_dotenv()

## Run once a day

ticker_symbols = ["NVDA","INTC","AMD"] # list of current ticker symbols

conn = intfunc.connect_db_pagila(os.getenv('DB_USER'), os.getenv('DB_PASS'))

#update data tables
for ticker_symbol in ticker_symbols:
  intfunc.truncate_finance_table(conn, ticker_symbol)

  fund_data = intfunc.get_fundamental_data(ticker_symbol)
  fund_df = intfunc.create_finance_dataframe(fund_data)
  intfunc.insert_values(fund_df, conn, "fundamental", ticker_symbol)
  
  income_data = intfunc.get_most_recent_income_statement(ticker_symbol)
  income_df = intfunc.create_finance_dataframe(income_data)
  intfunc.insert_values(income_df, conn, "income", ticker_symbol)

  balance_data = intfunc.get_most_recent_balance_sheet(ticker_symbol)
  balance_df = intfunc.create_finance_dataframe(balance_data)
  intfunc.insert_values(balance_df, conn, "balance", ticker_symbol)

  cashflow_data = intfunc.get_most_recent_cashflow_statement(ticker_symbol)
  cashflow_df = intfunc.create_finance_dataframe(cashflow_data)
  intfunc.insert_values(cashflow_df, conn, "cashflow", ticker_symbol)

  new_data = intfunc.get_recent_ticker_data(ticker_symbol)
  new_df = intfunc.create_ticker_dataframe(new_data)
  update_df = intfunc.df_to_update_ticker(new_df, conn, ticker_symbol)
  intfunc.insert_values(update_df, conn, "intrinsic", ticker_symbol)


conn.close()