import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')
outcomes_map = {'Rto-Adopt':1, 
                'Adoption':2, 
                'Euthanasia':3, 
                'Transfer':4,
                'Return to Owner':5, 
                'Died':6,
                'Disposal':7,
                'Missing': 8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}


def get_cred():
    bucket_name = "dtsclab3"
    credentials = {
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
    return credentials, bucket_name


def get_data_from_gcs(credentials, bucket_name):
    gcs_file_path = 'data/{}/outcomes_{}.csv'
    client = storage.Client.from_service_account_info(credentials)
    bucket = client.get_bucket(bucket_name)
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = gcs_file_path.format(current_date, current_date)
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df


def write_data_back(dataframe, credentials, bucket_name, path):
    print(f"Writing data to GCS.....")

    client = storage.Client.from_service_account_info(credentials)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")
    
    
def prep_data(data):
    data['name'] = data['name'].str.replace("*","",regex=False)
    data['sex'] = data['sex_upon_outcome'].replace({"Neutered Male":"M",
                                                    "Intact Male":"M", 
                                                    "Intact Female":"F", 
                                                    "Spayed Female":"F", 
                                                    "Unknown":np.nan})

    data['is_fixed'] = data['sex_upon_outcome'].replace({"Neutered Male":True,
                                                        "Intact Male":False, 
                                                        "Intact Female":False, 
                                                        "Spayed Female":True, 
                                                        "Unknown":np.nan})
    data['ts'] = pd.to_datetime(data.datetime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time
    data['outcome_type_id'] = data['outcome_type'].fillna("N/A")
    data['outcome_type_id'] = data['outcome_type'].replace(outcomes_map)

    return data


def transform_animal_dim(data):
    print("Preparing Animal Dimensions Table Data")
    animal_dim = data[['animal_id','name','date_of_birth', 'sex', 'animal_type', 'breed', 'color']]
    animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

    mode_sex = animal_dim['sex'].mode().iloc[0]
    animal_dim['sex'] = animal_dim['sex'].fillna(mode_sex)
    
    return animal_dim.drop_duplicates()


def transform_date_dim(data):
    print("Preparing Date Dimension Table Data")
    dates_dim = pd.DataFrame({
        'date_id':data.ts.dt.strftime('%Y%m%d'),
        'date':data.ts.dt.date,
        'year':data.ts.dt.year,
        'month':data.ts.dt.month,
        'day':data.ts.dt.day,
        })
    return dates_dim.drop_duplicates()


def transform_outcome_types_dim(data):
    print("Preparing Outcome Types Dimension Table Data")
    # map outcome string values to keys
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
    # keep only the necessary fields
    outcome_types_dim.columns=['outcome_type', 'outcome_type_id']    
    return outcome_types_dim


def transform_outcomes_fct(data):
    print("Preparing Outcome Fact Table Data")
    # pick the necessary columns and rename
    outcomes_fct = data[["animal_id", 'date_id','time', 'outcome_type_id', 'is_fixed']]
    return outcomes_fct


def transform_final_data():
    credentials, bucket_name = get_cred()

    new_data = get_data_from_gcs(credentials, bucket_name)
    
    new_data = prep_data(new_data)
    
    dim_animal = transform_animal_dim(new_data)
    dim_dates = transform_date_dim(new_data)
    dim_outcome_types = transform_outcome_types_dim(new_data)

    fct_outcomes = transform_outcomes_fct(new_data)

    dim_animal_path = "transformed_data/dim_animal.csv"
    dim_dates_path = "transformed_data/dim_dates.csv"
    dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
    fct_outcomes_path = "transformed_data/fct_outcomes.csv"

    write_data_back(dim_animal, credentials, bucket_name, dim_animal_path)
    write_data_back(dim_dates, credentials, bucket_name, dim_dates_path)
    write_data_back(dim_outcome_types, credentials, bucket_name, dim_outcome_types_path)
    write_data_back(fct_outcomes, credentials, bucket_name, fct_outcomes_path)