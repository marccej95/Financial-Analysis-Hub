import psycopg2
import psycopg2.extras as extras
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()



############################## Extract DATA - API ##############################

def get_full_ticker_data(ticker_symbol):
  """Gets full data to populate table"""
  try:
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker_symbol}&outputsize=full&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    full_data = response.json()
  except Exception as error:
    print(error)

  return full_data["Time Series (Daily)"]


def get_recent_ticker_data(ticker_symbol):
  """Gets recent data to update table"""
  try:
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker_symbol}&outputsize=compact&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    recent_data = response.json()
  except Exception as error:
    print(error)

  return recent_data["Time Series (Daily)"]


def get_fundamental_data(ticker_symbol):
  """Gets fundamental data for company overview"""
  try:
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker_symbol}&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    fundamental = response.json()
  except Exception as error:
    print(error)

  return fundamental


def get_most_recent_income_statement(ticker_symbol):
  """Gets most recent income statement"""
  try:
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker_symbol}&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    income = response.json()
  except Exception as error:
    print(error)

  return income["quarterlyReports"][0]


def get_most_recent_balance_sheet(ticker_symbol):
  """Gets most recent balance sheet"""
  try:
    url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker_symbol}&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    balance = response.json()
  except Exception as error:
    print(error)

  return balance["quarterlyReports"][0]


def get_most_recent_cashflow_statement(ticker_symbol):
  """Gets most recent cashflow statement"""
  try:
    url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker_symbol}&apikey={st.secrets['API_KEY']}"
    response = requests.get(url)
    cashflow = response.json()
  except Exception as error:
    print(error)

  return cashflow["quarterlyReports"][0]



############################## Extract DATA - Webscraping ##############################

def get_discount_rate(ticker_symbol):
  """Gets discount rate data from Alpha Spread"""
  try:
    url = f"https://www.alphaspread.com/security/nasdaq/{ticker_symbol}/discount-rate"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    discount = soup.findAll("div",class_="value weight-700")[4].text
    discount = discount.strip().replace("%","")
    discount_rate = float(discount)/100
    img_url = soup.find("img", class_="ui stock logo big image tooltip pointer").get("src")

  except Exception as error:
    print(error)

  return img_url, discount_rate


def get_data_for_calculation_yf_webscrape(ticker_symbol):
  """Gets next 5 years growth rate from Yahoo Finance by webscraping"""
  try:
    headers = {'User-Agent': 'Chrome/111.0.0.0'}
    url = f"https://uk.finance.yahoo.com/quote/{ticker_symbol}/analysis"
    response = requests.get(url, headers=headers)
  except Exception as error:
    print(error)
  
  soup = BeautifulSoup(response.text, "html.parser")
  cash_growth = float(soup.findAll("td", class_="Ta(end) Py(10px)")[16].text.replace("%",""))/100

  return cash_growth



############################## Transform data into dataframe ##############################

def create_ticker_dataframe(ticker_data):
  """Create dataframe to update table"""
  df_reversed = pd.DataFrame.from_dict(data=ticker_data, orient="index")
  df = df_reversed[::-1]
  df = df.reset_index()
  df.rename(columns={"index":"ticker_date", "1. open":"open", "2. high":"high", "3. low":"low", "4. close":"close", "5. volume":"volume"}, inplace=True)

  return df


def create_finance_dataframe(ticker_data):
  """Create dataframe to update table"""
  df = pd.DataFrame.from_dict(data=ticker_data, orient="index")
  df.reset_index(inplace=True)
  df.rename(columns={"index": "company_information", 0:"data"}, inplace=True)

  return df


def ticker_db_to_df(conn, ticker_symbol): 
  """Runs a SELECT query to get ticker_data from the database as a dataframe"""
  query = f"""
          SELECT * FROM student.mc_intrinsic_{ticker_symbol};
        """
  cur = conn.cursor()

  try:
    cur.execute(query)
  except Exception as error:
    print(error)

  rows = cur.fetchall()
  cur.close()

  rows_df = pd.DataFrame(rows)

  return rows_df


def finance_db_to_df(conn, table, ticker_symbol):
  """Runs a SELECT query to get financial data from the database as a dataframe"""
  query = f"""
          SELECT * FROM student.mc_{table}_{ticker_symbol};
        """
  cur = conn.cursor()

  try:
    cur.execute(query)
  except Exception as error:
    print(error)

  rows = cur.fetchall()
  cur.close()

  rows_df = pd.DataFrame(rows)

  return rows_df


def get_data_for_calculation(balance_df, cashflow_df, fundamental_df):
  """Gets all data needed for calculation from Alpha Vantage API"""
  oper_cash = float(cashflow_df.iloc[2][1])
  total_debt = float(balance_df.iloc[24][1]) + float(balance_df.iloc[27][1])
  cash_short_term_investment = float(balance_df.iloc[5][1])
  shares_outstanding = float(fundamental_df.iloc[48][1])

  return oper_cash, total_debt, cash_short_term_investment, shares_outstanding



############################## PostgreSQL database operations ##############################

def connect_db_pagila(db_user, db_pass):
  try:
    connect = psycopg2.connect(
        host = "data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
        database = "pagila",
        user = db_user,
        password = db_pass
    )
  except Exception as error:
     print(error)
  
  return connect


def create_ticker_table(conn, ticker_symbol):
  """Opens database connection, creates table, closes database connection"""
  query = f"""
          CREATE TABLE student.mc_intrinsic_{ticker_symbol}(
            ticker_date date PRIMARY KEY,
            open double precision,
            high double precision,
            low double precision,
            close double precision,
            volume double precision
          );
        """
  cur = conn.cursor()

  try:
    cur.execute(query)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cur.close()
    return 1

  print("The table has been created succesfully")
  cur.close()


def create_data_table(conn, table, ticker_symbol):
  """Opens database connection, creates table, closes database connection"""
  query = f"""
          CREATE TABLE student.mc_{table}_{ticker_symbol}(
            company_information text,
            data text
          );
        """
  cur = conn.cursor()

  try:
    cur.execute(query)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cur.close()
    return 1

  print("The table has been created succesfully")
  cur.close()
  

def insert_values(df, conn, table, ticker_symbol):
  """Using psycopg2.extras.execute_values() to insert the dataframe"""
  tuples = [tuple(x) for x in df.to_numpy()]
  cols = ','.join(list(df.columns))
  query  = "INSERT INTO %s(%s) VALUES %%s" % (f"student.mc_{table}_{ticker_symbol}", cols)
  cur = conn.cursor()
  
  try:
    extras.execute_values(cur, query, tuples)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cur.close()
    return 1
  
  print("execute_values() done")
  cur.close()


def df_to_update_ticker(df, conn, ticker_symbol):
  """Checks the index of the latest entry in the database, compares with the recent ticker data, and creates a dataframe that only has the values to be updated"""
  query = f"SELECT max(ticker_date) ::varchar FROM student.mc_intrinsic_{ticker_symbol};"
  cur = conn.cursor()

  try:
    cur.execute(query)
  except Exception as error:
    print(error)

  rows = cur.fetchall()
  cur.close()
  
  index = df.isin([rows[0][0]]).any(axis=1).idxmax()
  df_update = df[index+1::]

  return df_update


def truncate_finance_table(conn, ticker_symbol):
  """Truncates tables for new daily data to be inserted"""
  query = f"""TRUNCATE 
          student.mc_fundamental_{ticker_symbol}, 
          student.mc_income_{ticker_symbol},
          student.mc_balance_{ticker_symbol},
          student.mc_cashflow_{ticker_symbol}"""
  cur = conn.cursor()

  try:
    cur.execute(query)
    conn.commit()
  except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    conn.rollback()
    cur.close()
    return 1

  print("The table has been truncated succesfully")
  cur.close()



############################## Calculate data ##############################

def calculate_projected_discounted_cashflow(oper_cash, cash_growth, discount_rate):
  """Calculates projected and discounted cash flow for the next 20 years"""
  projected = []
  discount_factor = []

  year1_projected = oper_cash*(1+cash_growth)
  projected.append(year1_projected)
  for i in range(4):
    new_value = projected[-1]*(1+cash_growth)
    projected.append(new_value)
  for i in range(5):
    new_value = projected[-1]*(1+(cash_growth/2))
    projected.append(new_value)
  for i in range(10):
    new_value = projected[-1]*(1+0.04)
    projected.append(new_value) 

  year1_discount_factor = 1/(1+discount_rate)
  discount_factor.append(year1_discount_factor)
  for i in range(19):
    new_discount = discount_factor[-1]/(1+discount_rate)
    discount_factor.append(new_discount)

  discounted = [projected[i]*discount_factor[i] for i in range(len(projected))]

  return projected, discounted


def calculate_intrinsic_value(projected, discounted, total_debt, cash_short_term_investment, shares_outstanding):
  """Calculates intrinsic value based on total projected value and no. of shares outstanding, considering debt and investments"""
  total_pv = sum(projected) + sum(discounted)
  intrinsic = (total_pv - float(total_debt) + float(cash_short_term_investment))/float(shares_outstanding)

  return intrinsic



############################## Plot graphs ##############################

def projected_plot(projected, discounted): 
  """Plots line graph of projected and discounted cashflow values"""
  df = pd.DataFrame()
  df["year"] = [i for i in range(1,len(projected)+1)]
  df["projected"] = projected
  df["discounted"] = discounted

  fig = px.line(df, x="year", y=["projected","discounted"], markers=True)
  fig.update_layout(
      title="",
      xaxis_title="Year",
      yaxis_title="Cashflow Value",
      legend_title="Cashflow Type")

  return fig


def candlestick_plot(dataframe):
  """Produces a candlestick plot of the historical stock data"""
  fig = go.Figure(data=[go.Candlestick(x=dataframe[0],
                open=dataframe[1],
                high=dataframe[2],
                low=dataframe[3],
                close=dataframe[4])])
  fig.update_layout(
      title="",
      xaxis_title="Year",
      yaxis_title="Stock Price")

  return fig
