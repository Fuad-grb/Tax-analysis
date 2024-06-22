import requests
import pandas as pd

api_key = 'udGJsdZ2mKkME9QLuV751o28MkTYTxqr'
url = 'https://api.apilayer.com/tax_data/us_rate_list'

headers = {
    'apikey': api_key
}

params = {
    'state': 'CA'  
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    print("Data fetched successfully")
    
    print("Response keys:", data.keys())
    
   
    print(data)
    
    if 'taxes' in data:
        df = pd.DataFrame(data['taxes'])
        df.to_json('tax_data.json', orient='records')
        print("Data saved successfully in JSON format")
    else:
        print("Key 'taxes' not found in the response data")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.json())
