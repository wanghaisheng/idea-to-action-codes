import csv
import mysql.connector
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
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []
    finally:
        cursor.close()
        connection.close()

def load_undone_domains_cloudflare():
    """Load undone domains from Cloudflare D1."""
    try:
        zones = cf.zones.get()
        # Assuming Cloudflare D1 is connected to a zone and you have an API endpoint to get domains
        # Replace with actual implementation to fetch domains from D1
        response = cf.zones.get()  # Modify to actual endpoint call
        return [domain['name'] for domain in response['domains'] if domain['indexdate'] == 'unk']
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
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        connection.close()

def save_data_cloudflare(data):
    """Save data to Cloudflare D1."""
    try:
        # Implement saving to Cloudflare D1
        # For example, if Cloudflare D1 supports an API to save data:
        for record in data:
            cf.zones.post("your_endpoint", data=record)  # Modify with actual API endpoint and data format
    except cloudflare.exceptions.CloudflareAPIError as e:
        print(f"Cloudflare API error: {e}")

