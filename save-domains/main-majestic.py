import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from sqlalchemy.orm import aliased

# Define SQLAlchemy models
Base = declarative_base()

class UpdateHistory(Base):
    __tablename__ = 'majestic_domains_update_history'
    id = Column(Integer, primary_key=True)
    update_date = Column(Date, nullable=False)
    version = Column(String(50), nullable=False)
    description = Column(Text)

class Domain(Base):
    __tablename__ = 'majestic_domains'
    id = Column(Integer, primary_key=True)
    global_rank = Column(Integer)
    tld_rank = Column(Integer)
    domain = Column(String(255))
    tld = Column(String(10))
    ref_sub_nets = Column(Integer)
    ref_ips = Column(Integer)
    idn_domain = Column(String(255))
    idn_tld = Column(String(10))
    prev_global_rank = Column(Integer)
    prev_tld_rank = Column(Integer)
    prev_ref_sub_nets = Column(Integer)
    prev_ref_ips = Column(Integer)
    update_id = Column(Integer, ForeignKey('update_history.id'))
    update = relationship("UpdateHistory")

# Function to download the CSV file
def download_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)

# Function to insert data into the database
def insert_data(session, df):
    update_date = datetime.now().date()  # Use current date
    version = 'v1.0'  # Example version

    # Add a new update_history record
    update_history = UpdateHistory(update_date=update_date, version=version)
    session.add(update_history)
    session.commit()  # Commit to get the update_history id

    for _, row in df.iterrows():
        domain = Domain(
            global_rank=row['GlobalRank'],
            tld_rank=row['TldRank'],
            domain=row['domain'],
            tld=row['TLD'],
            ref_sub_nets=row['RefSubNets'],
            ref_ips=row['RefIPs'],
            idn_domain=row['IDN_Domain'],
            idn_tld=row['IDN_TLD'],
            prev_global_rank=row['PrevGlobalRank'],
            prev_tld_rank=row['PrevTldRank'],
            prev_ref_sub_nets=row['PrevRefSubNets'],
            prev_ref_ips=row['PrevRefIPs'],
            update_id=update_history.id
        )
        session.add(domain)

    session.commit()
# Function to query the latest row for a specific domain
def get_latest_domain_data(session, domain_name):
    latest_update = aliased(UpdateHistory)
    latest_domain = session.query(Domain).join(latest_update, Domain.update_id == latest_update.id).filter(Domain.domain == domain_name).order_by(latest_update.update_date.desc()).first()
    
    if latest_domain:
        update = session.query(UpdateHistory).get(latest_domain.update_id)
        return {
            'domain': latest_domain.domain,
            'global_rank': latest_domain.global_rank,
            'tld_rank': latest_domain.tld_rank,
            'ref_sub_nets': latest_domain.ref_sub_nets,
            'ref_ips': latest_domain.ref_ips,
            'update_date': update.update_date,
            'version': update.version
        }
    else:
        return None
# Main execution
def main():
    # Database configuration
    db_config = {
        'host': "gateway01.us-west-2.prod.aws.tidbcloud.com",
        'port': 4000,
        'user': "3i7meP2hYPkDk3V.root",
        'password': "xxxx",
        'database': "test",
        'ssl_ca': "./isrgrootx1.pem"
    }

    # Create SQLAlchemy engine
    engine = create_engine(
        f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?ssl_ca={db_config['ssl_ca']}",
        pool_size=10,
        max_overflow=20,
        pool_timeout=30
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the CSV file URL and local filename
    csv_url = 'https://downloads.majestic.com/majestic_million.csv'
    csv_filename = 'majestic_million.csv'

    # Download the CSV file
    download_csv(csv_url, csv_filename)

    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_filename)

    # Insert data into the database
    insert_data(session, df)

    # Close the session
    session.close()

if __name__ == '__main__':
    main()
