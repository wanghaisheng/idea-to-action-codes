import csv
import mysql.connector
from mysql.connector import Error
import cloudflare
from cloudflare import Cloudflare
import os

# MySQL configuration
mysql_config = {
    'host': "your_host",
    'user': "your_user",
    'password': "your_password",
    'database': "your_database"
}

# Cloudflare configuration
cf = Cloudflare(token='your_cloudflare_api_token')

def load_undone_domains_csv(filepath):
    """Load undone domains from a CSV file."""
    try:
        with open(filepath, mode='r') as file:
            reader = csv.DictReader(file)
            return [row['domain'] for row in reader if row['indexdate'] == 'unk']
    except FileNotFoundError:
        print(f"File {filepath} not found.")
        return []

def load_undone_domains_mysql():
    """Load undone domains from MySQL."""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        query = "SELECT domain FROM domain_index_data WHERE indexdate = 'unk'"
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    except Error as err:
        print(f"Error: {err}")
        return []
    finally:
        cursor.close()
        connection.close()

def load_undone_domains_cloudflare():
    """Load undone domains from Cloudflare D1."""
    try:
        # Replace with actual endpoint call to Cloudflare D1 if available
        response = cf.zones.get()  # Modify with actual endpoint call
        return [domain['name'] for domain in response if domain.get('indexdate') == 'unk']
    except cloudflare.exceptions.CloudflareAPIError as e:
        print(f"Cloudflare API error: {e}")
        return []

def save_data_csv(filepath, data):
    """Save data to a CSV file."""
    with open(filepath, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        if os.stat(filepath).st_size == 0:
            writer.writeheader()
        writer.writerows(data)

def save_data_mysql(data):
    """Save data to MySQL."""
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        for record in data:
            cursor.execute(
                """
                INSERT INTO domain_index_data (domain, indexdate, Aboutthesource, Intheirownwords)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                indexdate = VALUES(indexdate),
                Aboutthesource = VALUES(Aboutthesource),
                Intheirownwords = VALUES(Intheirownwords)
                """,
                (record['domain'], record['indexdate'], record['Aboutthesource'], record['Intheirownwords'])
            )
        connection.commit()
    except Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()

def save_data_cloudflare(data):
    """Save data to Cloudflare D1."""
    try:
        # Implement saving to Cloudflare D1
        # Example: replace with actual API endpoint and data format
        for record in data:
            cf.zones.post("your_endpoint", data=record)  # Modify with actual API endpoint
    except cloudflare.exceptions.CloudflareAPIError as e:
        print(f"Cloudflare API error: {e}")

