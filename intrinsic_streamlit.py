import streamlit as st
import os
import intrinsic_functions as intfunc
from dotenv import load_dotenv

load_dotenv()

########## Data visualisation for streamlit ##########

st.title("Stock Analysis Hub")

# Selection for ticker_symbol, will be used throughout the script
ticker_symbol = st.selectbox("Ticker Symbol", options=["NVDA", "INTC", "AMD"])

# Extract latest data from API/webscraping
img_url, discount_rate = intfunc.get_discount_rate(ticker_symbol)

# Get data from PostgreSQL
conn = intfunc.connect_db_pagila(st.secrets["DB_USER"], st.secrets["DB_PASS"])
ticker_df = intfunc.ticker_db_to_df(conn, ticker_symbol)

fundamental_df = intfunc.finance_db_to_df(conn, "fundamental", ticker_symbol)
income_df = intfunc.finance_db_to_df(conn, "income", ticker_symbol)
balance_df = intfunc.finance_db_to_df(conn, "balance", ticker_symbol)
cashflow_df = intfunc.finance_db_to_df(conn, "cashflow", ticker_symbol)
currency = fundamental_df.iloc[6][1]

oper_cash, total_debt, cash_short_term_investment, shares_outstanding = intfunc.get_data_for_calculation(balance_df, cashflow_df, fundamental_df)
cash_growth = intfunc.get_data_for_calculation_yf_webscrape(ticker_symbol)

latest_price = ticker_df.iloc[-1][4]

# Data calculation
projected, discounted = intfunc.calculate_projected_discounted_cashflow(oper_cash, cash_growth, discount_rate)
intrinsic_value = intfunc.calculate_intrinsic_value(projected, discounted, total_debt, cash_short_term_investment, shares_outstanding)

fig_projected = intfunc.projected_plot(projected, discounted)
fig_candle = intfunc.candlestick_plot(ticker_df)

amount = abs(intrinsic_value - float(latest_price))
amount = currency + " " + format(amount , '.2f')

if float(latest_price) < intrinsic_value:
  ratio = "UNDERVALUED by"
else:
  ratio = "OVERVALUED by"

latest_price = currency + " " + format(float(latest_price), '.2f')
intrinsic_value  = currency + " " + format(intrinsic_value , '.2f')

col_left, col_middle, col_right = st.columns([0.2, 0.4, 0.4])

with col_left:
  st.image(img_url)

with col_middle:
  st.subheader("Latest Stock Price")
  st.subheader("Intrinsic Value")
  st.subheader("Stock is Currently")

with col_right:
  st.subheader(": " + latest_price)
  st.subheader(": " + intrinsic_value)
  st.subheader(": " + ratio)
  st.subheader(": " + amount)

st.table(data=fundamental_df.iloc[0:10])
st.subheader("Projected & Discounted Cashflow Value for the Next 20 Years")
st.plotly_chart(fig_projected)
st.subheader(f"Historical Data for {ticker_symbol}")
st.plotly_chart(fig_candle)

st.header("Company Financials Most Recent Quarter")
finance_choice = st.radio("", options=["Income Statement", "Balance Sheet", "Cashflow Statement"])

if finance_choice == "Income Statement":
  doc_choice = income_df
elif finance_choice == "Balance Sheet":
  doc_choice = balance_df
else:
  doc_choice = cashflow_df

st.table(data=doc_choice)

conn.close()
