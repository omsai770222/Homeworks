import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCSDataLoader:

    def __init__(self):
        self.bucket_name = 'dtsclab3'

    def get_credentials(self):
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
        return credentials_info

    def connect_to_gcs_and_get_data(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        credentials_info = self.get_credentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCS into a DataFrame
        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcs_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',
            'host': '34.31.45.85',
            'port': '5432',
        }

    def get_queries(self, table_name):
        if table_name =="dim_animals":
            query = """CREATE TABLE IF NOT EXISTS dim_animals (
                            animal_id VARCHAR(7) PRIMARY KEY,
                            name VARCHAR,
                            dob DATE,
                            sex VARCHAR(1), 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR
                        );
                        """
        elif table_name =="dim_outcome_types":
            query = """CREATE TABLE IF NOT EXISTS dim_outcome_types (
                            outcome_type_id INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
        elif table_name =="dim_dates":
            query = """CREATE TABLE IF NOT EXISTS dim_dates (
                            date_id VARCHAR(8) PRIMARY KEY,
                            date DATE NOT NULL,
                            year INT2  NOT NULL,
                            month INT2  NOT NULL,
                            day INT2  NOT NULL
                        );
                        """
        else:
            query = """CREATE TABLE IF NOT EXISTS fct_outcomes (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_id VARCHAR(7) NOT NULL,
                            date_id VARCHAR(8) NOT NULL,
                            time TIME NOT NULL,
                            outcome_type_id INT NOT NULL,
                            is_fixed BOOL,
                            FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
                            FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
                            FOREIGN KEY (outcome_type_id) REFERENCES dim_outcome_types(outcome_type_id)
                        );
                        """
        return query

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        print("table_query:", table_query)
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcs_data)}")
        
def load_data_to_postgres_main(file_name, table_name):
    gcs_loader = GCSDataLoader()
    table_data_df = gcs_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)