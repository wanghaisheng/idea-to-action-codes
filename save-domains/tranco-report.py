import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, aliased
from sqlalchemy.sql import func
from datetime import datetime, timedelta
import schedule
import time

# Define SQLAlchemy models
Base = declarative_base()

class UpdateHistory(Base):
    __tablename__ = 'update_history'
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

class RankReport(Base):
    __tablename__ = 'rank_reports'
    id = Column(Integer, primary_key=True)
    period = Column(String(50))
    report_date = Column(Date, nullable=False)
    data = Column(String(4000))  # Store JSON data or a serialized format

# Function to download the CSV file
def download_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)

# Function to insert data into the database
def insert_data(session, df):
    update_date = datetime.now().date()
    version = f'v{update_date.strftime("%Y%m")}'
    update_history = UpdateHistory(update_date=update_date, version=version, description='Monthly import')
    session.add(update_history)
    session.commit()

    for _, row in df.iterrows():
        domain = TrancoDomain(
            rank=row['rank'],
            domain=row['domain'],
            update_id=update_history.id
        )
        session.add(domain)
    session.commit()

# Function to calculate rank differences
def rank_difference_report(session, start_date, end_date, top_n):
    results = []

    subquery = (
        session.query(
            TrancoDomain.domain,
            TrancoDomain.rank.label('current_rank'),
            UpdateHistory.version.label('current_version'),
            func.max(UpdateHistory.update_date).label('latest_update_date')
        )
        .join(UpdateHistory, TrancoDomain.update_id == UpdateHistory.id)
        .filter(TrancoDomain.rank <= top_n, UpdateHistory.update_date.between(start_date, end_date))
        .group_by(TrancoDomain.domain, TrancoDomain.rank, UpdateHistory.version)
        .subquery()
    )

    current_rank = aliased(TrancoDomain, alias=subquery)
    previous_rank = aliased(TrancoDomain, alias=subquery)

    rank_diffs = (
        session.query(
            current_rank.domain,
            current_rank.current_rank,
            previous_rank.current_rank.label('previous_rank'),
            (previous_rank.current_rank - current_rank.current_rank).label('rank_difference'),
            current_rank.current_version,
            previous_rank.current_version.label('previous_version')
        )
        .outerjoin(previous_rank, (current_rank.domain == previous_rank.domain) &
                                  (current_rank.current_version > previous_rank.previous_version))
        .filter(current_rank.current_rank <= top_n)
        .order_by(current_rank.domain, current_rank.current_version)
        .all()
    )

    for row in rank_diffs:
        results.append({
            'domain': row.domain,
            'current_rank': row.current_rank,
            'previous_rank': row.previous_rank,
            'rank_difference': row.rank_difference,
            'current_version': row.current_version,
            'previous_version': row.previous_version
        })

    return results

# Function to generate and save reports
def generate_reports(session):
    today = datetime.now().date()
    periods = {
        '1 month': (today - timedelta(days=30), today),
        '3 months': (today - timedelta(days=90), today),
        '6 months': (today - timedelta(days=180), today)
    }

    for period, (start_date, end_date) in periods.items():
        for top_n in [100, 10000]:
            differences = rank_difference_report(session, start_date, end_date, top_n)
            report_data = str(differences)  # Serialize the data
            report = RankReport(period=period, report_date=today, data=report_data)
            session.add(report)
        session.commit()

# Scheduled job
def job():
    DATABASE_URL = 'mysql+pymysql://username:password@host/dbname'
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    csv_url = 'https://example.com/cloudflare_data.csv'
    csv_filename = 'cloudflare_data.csv'

    download_csv(csv_url, csv_filename)
    df = pd.read_csv(csv_filename)
    insert_data(session, df)
    generate_reports(session)
    session.close()

# Schedule the job to run on the 1st of every month
schedule.every().month.at("00:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
