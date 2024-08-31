import requests
import pandas as pd
import zipfile
import io
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# Define SQLAlchemy models
Base = declarative_base()

class UpdateHistory(Base):
    __tablename__ = 'tranco_domains_update_history'
    id = Column(Integer, primary_key=True)
    update_date = Column(Date, nullable=False)
    version = Column(String(50), nullable=False)
    description = Column(String(255))

class TrancoDomain(Base):
    __tablename__ = 'tranco_domains'
    id = Column(Integer, primary_key=True)
    rank = Column(Integer)
    domain = Column(String(255))
    update_id = Column(Integer, ForeignKey('tranco_domains_update_history.id'))
    update = relationship("UpdateHistory")

# Function to download and extract the ZIP file
def download_and_extract_zip(url):
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # List all files in the ZIP archive
        file_names = z.namelist()
        # Assume the first file is the CSV file
        csv_file_name = file_names[0]
        # Extract the CSV file content
        with z.open(csv_file_name) as file:
            df = pd.read_csv(file)
    return df

# Function to insert data into the database
def insert_data(session, df):
    # Add a new update_history record
    update_date = datetime.now().date()  # Use the current date
    version = 'v1.0'  # Example version
    update_history = UpdateHistory(update_date=update_date, version=version, description='Initial import')
    session.add(update_history)
    session.commit()  # Commit to get the update_history id

    # Insert new domain data
    for _, row in df.iterrows():
        domain = TrancoDomain(
            rank=row['rank'],
            domain=row['domain'],
            update_id=update_history.id
        )
        session.add(domain)
    session.commit()

# Function to query the latest rank for a specific domain
def get_latest_rank(session, domain_name):
    latest_domain = session.query(TrancoDomain).join(UpdateHistory, TrancoDomain.update_id == UpdateHistory.id).filter(TrancoDomain.domain == domain_name).order_by(UpdateHistory.update_date.desc()).first()
    
    if latest_domain:
        update = session.query(UpdateHistory).get(latest_domain.update_id)
        return {
            'domain': latest_domain.domain,
            'rank': latest_domain.rank,
            'update_date': update.update_date,
            'version': update.version
        }
    else:
        return None

# Main execution
def main():
    # Database configuration
    db_config = {
        'host': "your-postgres-host",
        'port': 5432,
        'user': "your-username",
        'password': "your-password",
        'database': "your-database"
    }

    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}",
        pool_size=10,
        max_overflow=20,
        pool_timeout=30
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the ZIP file URL
    zip_url = 'https://tranco-list.eu/download/YXW2G/full.zip'

    # Download and extract the ZIP file
    df = download_and_extract_zip(zip_url)

    # Insert data into the database
    insert_data(session, df)

    # Query the latest rank for a specific domain
    domain_name = 'google.com'  # Change this to the domain you want to query
    latest_data = get_latest_rank(session, domain_name)

    if latest_data:
        print(f"Latest data for {domain_name}:")
        print(f"Rank: {latest_data['rank']}")
        print(f"Update Date: {latest_data['update_date']}")
        print(f"Version: {latest_data['version']}")
    else:
        print(f"No data found for domain {domain_name}.")

    # Close the session
    session.close()

if __name__ == '__main__':
    main()
