# dbhelper.py

import pandas as pd
import mysql.connector
from mysql.connector import Error
import cloudflare_d1  # Hypothetical library

# Function to load data from CSV
def load_data_csv(filepath):
    return pd.read_csv(filepath, usecols=["domain"])

# Function to load data from MySQL
def load_data_mysql():
    db_config = {
        'host': "your_mysql_host",
        'port': 3306,
        'user': "your_mysql_user",
        'password': "your_mysql_password",
        'database': "your_mysql_database"
    }
    
    query = "SELECT domain FROM domain_index_data WHERE indexdate IS NULL OR indexdate = ''"
    
    try:
        connection = mysql.connector.connect(**db_config)
        df = pd.read_sql(query, connection)
        return df
    except Error as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    finally:
        if connection.is_connected():
            connection.close()

# Function to load data from Cloudflare D1
def load_data_cloudflare_d1():
    # Hypothetical function for Cloudflare D1
    client = cloudflare_d1.Client("your_api_key")
    data = client.query("SELECT domain FROM your_table WHERE indexdate IS NULL OR indexdate = ''")
    return pd.DataFrame(data)

# Function to save data to CSV
def save_data_csv(filepath, data):
    data.to_csv(filepath, index=False)

# Function to save data to MySQL
def save_data_mysql(data):
    db_config = {
        'host': "your_mysql_host",
        'port': 3306,
        'user': "your_mysql_user",
        'password': "your_mysql_password",
        'database': "your_mysql_database"
    }
    
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        for index, row in data.iterrows():
            cursor.execute("""
                INSERT INTO domain_index_data (domain, indexdate, Aboutthesource, Intheirownwords)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE indexdate=VALUES(indexdate), Aboutthesource=VALUES(Aboutthesource), Intheirownwords=VALUES(Intheirownwords)
            """, (row['domain'], row['indexdate'], row['Aboutthesource'], row['Intheirownwords']))
        
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to save data to Cloudflare D1
def save_data_cloudflare_d1(data):
    # Hypothetical function for Cloudflare D1
    client = cloudflare_d1.Client("your_api_key")
    for index, row in data.iterrows():
        client.insert_or_update("your_table", {
            'domain': row['domain'],
            'indexdate': row['indexdate'],
            'Aboutthesource': row['Aboutthesource'],
            'Intheirownwords': row['Intheirownwords']
        })
