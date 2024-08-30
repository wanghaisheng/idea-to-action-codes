# dbhelper.py
import mysql.connector
from mysql.connector import Error

class MySQLHelper:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS domains (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(255) NOT NULL,
            tld VARCHAR(255),
            title TEXT,
            description TEXT,
            raw TEXT,
            language VARCHAR(10)
        )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            self.connection.commit()

    def add_domain(self, domain):
        insert_query = """
        INSERT INTO domains (url, tld, title, description, raw, language)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (domain.url, domain.tld, domain.title, domain.description, domain.raw, domain.language)
        with self.connection.cursor() as cursor:
            cursor.execute(insert_query, values)
            self.connection.commit()

    def close(self):
        if self.connection.is_connected():
            self.connection.close()


# dbhelper.py (extend this file)
from cloudflare_d1 import CloudflareD1

class D1Helper:
    def __init__(self, api_token, database_id):
        self.client = CloudflareD1(api_token)
        self.database_id = database_id
        self.setup()

    def setup(self):
        # Define the schema for the table
        schema = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "url": "TEXT NOT NULL",
            "tld": "TEXT",
            "title": "TEXT",
            "description": "TEXT",
            "raw": "TEXT",
            "language": "TEXT"
        }
        self.client.create_table(self.database_id, "domains", schema)

    def add_domain(self, domain):
        insert_data = {
            "url": domain.url,
            "tld": domain.tld,
            "title": domain.title,
            "description": domain.description,
            "raw": domain.raw,
            "language": domain.language
        }
        self.client.insert(self.database_id, "domains", insert_data)

    def close(self):
        pass  # No action needed for Cloudflare D1 on close
