import pandas as pd
from pandas_gbq import read_gbq, to_gbq


PROJECT_ID = "gorgias-case-study-454321"
DATASET = "gorgias_growth"

def send_to_bigquery(df: pd.DataFrame, table_name: str):
    """Use pandas_gbq to send dataframe to Bigquery"""
    destination = f"{PROJECT_ID}.{DATASET}.{table_name}"
    to_gbq(df, destination, project_id=PROJECT_ID, if_exists='replace')  # Adjust if_exists as needed



df = pd.read_csv("customers_enriched.csv")
send_to_bigquery(df, "customers_enriched")
