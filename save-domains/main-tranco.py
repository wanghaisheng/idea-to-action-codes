import requests
import pandas as pd
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
    update_id = Column(Integer, ForeignKey('update_history.id'))
    update = relationship("UpdateHistory")

# Function to download the CSV file
def download_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)

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
    latest_update = aliased(UpdateHistory)
    latest_domain = session.query(TrancoDomain).join(latest_update, TrancoDomain.update_id == latest_update.id).filter(TrancoDomain.domain == domain_name).order_by(latest_update.update_date.desc()).first()
    
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
    DATABASE_URL = 'mysql+pymysql://username:password@host/dbname'

    # Create the database engine and session
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the CSV file URL and local filename
    csv_url = 'https://tranco-list.eu/download/YXW2G/full'
    csv_filename = 'tranco_full.csv'

    # Download the CSV file
    download_csv(csv_url, csv_filename)

    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_filename)

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
