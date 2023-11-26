import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage


# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')


def extract_data_from_api(limit=50000, order='animal_id'):
    """
    Function to extract data from data.austintexas.gov API.
    """
    base_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    
    api_key = '316jbwapzkn65aclsprhx0c8m'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    
    offset = 0
    all_data = []

    while offset < 157000:  # Assuming there are 157k records
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': order,
        }

        response = requests.get(base_url, headers=headers, params=params)
        print("response : ", response)
        current_data = response.json()
        
        # Break the loop if no more data is returned
        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data


def create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    data_list = []
    for entry in data:
        row_data = [entry.get(column, None) for column in columns]
        data_list.append(row_data)

    df = pd.DataFrame(data_list, columns=columns)
    return df


def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    print("Writing data to GCS.....")
    credentials_info = {
                           "type": "service_account",
                            "project_id": "inner-bot-406207",
                            "private_key_id": "c29939c0f18bc86d8bdaa3f3024156da285319e0",
                            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCrSBWHaGuTjNG+\nyy17Z4Y3RtTYPFzTuMKuaOGQr1FxFP0VkCPl7tjnb/ICinCmnQ2lPcIcOLx8qhb8\njORXrFb6tXiuB5IvRtMFDHcXr6vCTt+ffiSBmR+W2StNGdLA232+5B1ecRUlLkxO\nNzwGj6WQq7eBnXZxXI8TzQJiqK7lLK9Ty23AWXjmXRCxLwydsO+jwpftR50pseDc\n41iO3oHlbj+hj0jcSUjAWvn1zc0jq2nF7WqZRRyYHL8jqpMXP4/8oQWAjozViSk/\nf+Qt5sOD0RMTGTSGU1OYkIjzDkd6PD//FhiYT3SshqqGCsfNd97R5u4DUo2w48Ep\nOeehH5jrAgMBAAECggEAC5736viAOAIZZv8FBnbnKc/x9/vcdnb2KnwuNbFXbrs0\nUqa1cM/MQqObQWXgL8W/G8N9bInC7BhkN91dTr+AJUFzvyb/WZrODW0jlWVTW3sb\nfAhEdNCNTqBMak5CuNgLh4yVm4dDnoKuJa5ksbNEUfT8ndwvoZBcmsTRkR0NxLmv\nNj8V2dzX/PozjVm63rET2CKwcOwseIl7YPPt/k1+Gsb1da43AnvZiYv8KA+X2ofp\n8v/f09cuBv3Uu8SLo2ySDsSRBZEMb0stnggvJ70awp0Wq+AE/8PJtp+QunA1WE65\nGbFtu4C/zDRBuqviIn3GZ44pQ2wqBqLs3jJGZtT/3QKBgQDxSIuJSZh5Yla5KAPW\nZbHZ4xRzHEkOA2JgKADh1AY255jpAcmt3rBM9DuGofG0aE17/y/CYt6+BTEg5t1L\nLgysPtNs5J0h9f8Q3YD682jW/WMM/iZ7dWSS2G6QLUn4deCDbVuEz7dHxXYJnQti\nt9839ZT0+EJsxlFvVdU4StW3nwKBgQC1uoO8rwVJ6SuXGAVYOGcIGHLs2T/h3tX/\nvviTNx4p3w6GHTbXX6cz91SUOmXf5lvbeRWjn3/pLAQhpJ6QxZPffYPqEVwhMDjV\n5vkOe58HV3zGYQU2EgKPNrfB56cEK7SxWOb4e7wrlMK8UbeVTyzD6ol75xV1A4Du\npgvJNctLNQKBgAoNBZMAd9OGnyozWoWR0ujKps1svjZROcXbpL3DgUbEOwBpzKbr\ngdiSy+/9yv3C4odpx+mHCNRNjxBZzZoWHv0F2PTOSSVjA+8F6xuJDDL4dynRZHT8\nHOZpEaH74PrynhmbcpaDuJTKyvH3ksPX+PpxxnS16r5xPG8w2iLwKxrtAoGARts3\nXE1doYjhryL1ioVLXvOxc7ntV5M8G2CJqVTXVPocvpwLcwRfvDpWZwkyCY/XBwb6\nMfcRt4erALcgAFCZLI3S7mPgaSxVLMnHGCeJRSKOiwbvMjrpdo+eLGO3Uj/8Tx3b\nyMzt/IwfNbAjBXt+d9Et0/qw+hPopKRnegeYgXUCgYBGd+OlngaXNImWI45z7y0J\nWsgqGNMA19uR0wc4aDvBvsuk3zXm7/OWBwO6truduT8fY49xavC/SBFedDX9kL00\n62rUic95nMvfZkRlx7rVxTEn2FnPuCzNn+jaOU3EeMKkAsJuwsO6u9AipcWva3NE\nFcl8DVgLMphk9d4hINNzmw==\n-----END PRIVATE KEY-----\n",
                            "client_email": "dtsclab3@inner-bot-406207.iam.gserviceaccount.com",
                            "client_id": "104234083795094642851",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dtsclab3%40inner-bot-406207.iam.gserviceaccount.com",
                            "universe_domain": "googleapis.com"
                        }

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS with date: {current_date}.")


def main():
    extracted_data = extract_data_from_api(limit=50000, order='animal_id')
    shelter_data = create_dataframe(extracted_data)

    gcs_bucket_name = 'dtsclab3'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    upload_to_gcs(shelter_data, gcs_bucket_name, gcs_file_path)