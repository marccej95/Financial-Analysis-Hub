This is a project that aims to be a financial hub, allowing users to:
1. Analyse a company's financials - updated daily
2. View company stock price history - updated daily
3. Use discounted cashflow method to evaluate company stock intrinsic value - updated with most recent values from several sources


The app is hosted on Streamlit, running on the intrinsic_streamlit.py file

When setting up a new company's ticker symbol, run the intrinsic_first_time_ticker.py file and change the ticker symbol at the top of the file.

There is a CRON job hosted on AWS EC2 that runs intrinsic_daily_run.py daily to update the database with the latest information.
