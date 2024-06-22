import pandas as pd
import ast
from sklearn.preprocessing import StandardScaler
import boto3
from io import BytesIO

tax_data = pd.read_csv(r'C:\Users\hp\OneDrive\Рабочий стол\unibank\tax_data.csv')

tax_data = tax_data.drop_duplicates()

def parse_standard_rate(rate_str):   #rate cədvəllərindən sırf rate dəyərlərin parse edirik
    try:
        rate_dict = ast.literal_eval(rate_str)
        return rate_dict.get('rate', None)
    except (ValueError, SyntaxError):
        return None

def parse_other_rates(other_rates_str):
    try:
        other_rates = ast.literal_eval(other_rates_str)
        if isinstance(other_rates, list):
            return [rate.get('rate', None) for rate in other_rates if 'rate' in rate]
        else:
            return []
    except (ValueError, SyntaxError):
        return []

tax_data['standard_rate'] = tax_data['standard_rate'].apply(parse_standard_rate)
tax_data['other_rates'] = tax_data['other_rates'].apply(parse_other_rates)

other_rates_df = tax_data['other_rates'].apply(pd.Series)
other_rates_df.columns = [f'other_rate_{i+1}' for i in range(other_rates_df.shape[1])]

#təzə dataframe yaradırıq, yeni cədvəlləri əlavə edib kohnəni silirik

tax_data_cleaned = pd.concat([tax_data, other_rates_df], axis=1) 
tax_data_cleaned = tax_data_cleaned.drop(columns=['other_rates'])

tax_data_cleaned['standard_rate'] = tax_data_cleaned['standard_rate'].fillna(tax_data_cleaned['standard_rate'].median())
for col in other_rates_df.columns:
    tax_data_cleaned[col] = tax_data_cleaned[col].fillna(tax_data_cleaned[col].median())

#yeni cədvəl yaradırıq birdən çox other_rates dəyəri olanlar üçün
tax_data_cleaned['multiple_other_rates'] = tax_data_cleaned[other_rates_df.columns].notnull().sum(axis=1) > 1


scaler = StandardScaler()
rate_columns = ['standard_rate'] + [col for col in tax_data_cleaned.columns if col.startswith('other_rate')]
tax_data_cleaned[rate_columns] = scaler.fit_transform(tax_data_cleaned[rate_columns])

processed_csv_path = "processed_tax_data.csv"
processed_parquet_path = "processed_tax_data.parquet"

tax_data_cleaned.to_csv(processed_csv_path, index=False)
tax_data_cleaned.to_parquet(processed_parquet_path, index=False)

#minioya save edirik
s3_client = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',  
    aws_access_key_id='minioadmin',  
    aws_secret_access_key='minioadmin'  
)

csv_buffer = BytesIO()
tax_data_cleaned.to_csv(csv_buffer, index=False)
s3_client.put_object(Bucket='taxbucket', Key='processed_tax_data.csv', Body=csv_buffer.getvalue())

parquet_buffer = BytesIO()
tax_data_cleaned.to_parquet(parquet_buffer, index=False)
s3_client.put_object(Bucket='taxbucket', Key='processed_tax_data.parquet', Body=parquet_buffer.getvalue())

print("Data successfully saved to MinIO")
