import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine



df=pd.read_csv("https://shelterdata.s3.amazonaws.com/shelter1000.csv")

# Data Cleaning & Tranformation:
def Transform(df):

    new_df=df.copy()

    #Removing duplicate Values based on Animal_ID
    new_df = new_df.drop_duplicates(subset='Animal ID', keep='first')
    new_df = new_df.drop_duplicates(subset='DateTime', keep='first')

    # Handling incorrect values in Name column
    new_df['Name'] = new_df['Name'].mask(new_df['Name'] == new_df['Animal ID'], np.nan)
    new_df['DateTime'] = pd.to_datetime(df['DateTime'])

    def age_in_years(age):
        age_in_years = 0
        if isinstance(age, str):
            if 'year' in age:
                age_in_years = int(age.split()[0])
            elif 'month' in age:
                age_in_months = int(age.split()[0])
                age_in_years = age_in_months/12
            elif 'day' in age:
                age_in_days = int(age.split()[0])
                age_in_years = age_in_days/365
            

        if age_in_years < 1:
            age_in_years = 0

        return str(age_in_years)


    new_df['Age'] = new_df['Age upon Outcome'].apply(age_in_years)


    # Splitting the Sex into two separate columns as Sterilization_Status and Sex
    new_df[['Sterilization_Status', 'Sex']] = new_df['Sex upon Outcome'].str.split(' ',  expand=True)
    new_df[['Month', 'Year']] = new_df['MonthYear'].str.split(' ', expand=True)
   
    new_df.drop(columns=['Age upon Outcome','Sex upon Outcome', 'MonthYear'], inplace=True)

    #Renaming all the columns to make them suitable for schema creation in Database
    new_df.rename(columns={'Animal ID': 'Animal_ID','Animal Type':'Animal_Type','Date of Birth':'Date_of_Birth','Outcome Type':'Outcome_Type','Outcome Subtype':'Outcome_SubType'}, inplace=True)

    return new_df


def insert_tables(cur, conn, final_df):
    for index, row in final_df.iterrows():
        dim_Date_insert = """
        INSERT INTO dim_Date (DateTime, Time, Month, Day, Year, Is_Weekend)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        dim_Animal_Info_insert = """
            INSERT INTO dim_Animal_Info (Animal_ID, Name, Date_of_Birth, Animal_Type, Age)
            VALUES (%s, %s, %s, %s, %s)
            """

        dim_Animal_char_insert = """
            INSERT INTO dim_Animal_char (Animal_ID, Breed, Color, Sterilization_Status, Sex)
            VALUES (%s, %s, %s, %s, %s)
            """

        fact_Outcome_Info_insert= """
            INSERT INTO fact_Outcome_Info ( Animal_ID, DateTime, Outcome_Type, Outcome_SubType)
            VALUES (%s, %s, %s, %s)
            """
        # Assuming you can extract the required values from your 'outcome_info' DataFrame
        dim_Date_data = (
            row["DateTime"],  # Replace with the correct column name
            row["DateTime"].time(),  # Extract time from DateTime
            row['Month'],  # Extract month name
            row["DateTime"].day,  # Extract day
            row["Year"],  # Extract year
            row["DateTime"].weekday() >= 5  # Check if it's a weekend (0=Monday, 6=Sunday)
        )

        dim_animal_Info_data = (row["Animal_ID"], row["Name"], row["Date_of_Birth"], row["Animal_Type"], row["Age"])
        dim_animal_char_data = (row["Animal_ID"], row["Breed"], row["Color"], row["Sterilization_Status"], row["Sex"])
        dim_Outcome_Info_data = (row["Animal_ID"], row["DateTime"], row["Outcome_Type"], row["Outcome_SubType"])        
        

        cur.execute(dim_Date_insert, dim_Date_data)
        cur.execute(dim_Animal_Info_insert, dim_animal_Info_data)
        cur.execute(dim_Animal_char_insert, dim_animal_char_data)
        cur.execute(fact_Outcome_Info_insert, dim_Outcome_Info_data)

        
        conn.commit()
        


def main():

    db_params = {
    'database': 'shelter',
    'user': 'omsai',
    'password': 'simple123',
    'host': 'db',
    'port': '5432'
    }

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    final_df=Transform(df)
    insert_tables(cur, conn, final_df)
    conn.close()


    conn.close()

if __name__ == "__main__":
    main()
