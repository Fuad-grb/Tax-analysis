import requests
import pandas as pd

def fetch_tax_data(api_url, api_key):
    headers = {
        'apikey': api_key
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data["data"])
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

api_url = "https://api.apilayer.com/tax_data/rate_list?per_page=100" 
api_key = "udGJsdZ2mKkME9QLuV751o28MkTYTxqr"  
tax_data_df = fetch_tax_data(api_url, api_key)

print(tax_data_df.head())

tax_data_df.to_csv("tax_data.csv", index=False)
tax_data_df.to_parquet("tax_data.parquet", index=False)