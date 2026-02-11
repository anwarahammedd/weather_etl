import requests
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO,
                    filename='weather.log',
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL")
CITIES = ["Doha", "New York", "Tokyo", "London", "Sydney"]

# Create DB engine
engine = create_engine(DB_URL)

# Create tables if they don't exist
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS raw_weather(
        id SERIAL PRIMARY KEY,
        city VARCHAR(255),
        fetch_time TIMESTAMP,
        raw_data JSONB
    );"""
    ))
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS transformed_weather(
        id SERIAL PRIMARY KEY,
        city VARCHAR(255),
        date DATE,
        avg_temp FLOAT,
        avg_humidity FLOAT
    );"""))
    conn.commit()

def fetch_weather(city):
    logging.info(f"ETL Started at {datetime.now()}")
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data for {city}: {response.text}")
        logging.error(f"Error fetching weather data for {city}: {response.text}")
        return None

def store_raw_data(city, data):
    fetch_time = datetime.now()
    fetch_date = fetch_time.date()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1
            FROM raw_weather
            WHERE city = :city
            AND DATE(fetch_time) = :fetch_date
        """),{
            "city": city,
            "fetch_date": fetch_date,
        }).fetchone()
        if result:
            print(f"ETL already completed for {city} on {fetch_date}")
            logging.error(f"ETL already completed for {city} on {fetch_date}")
            return

        conn.execute(text("""
        INSERT INTO raw_weather (city, fetch_time, raw_data)
        values (:city, :fetch_time, :raw_data)"""),
                     {
                         "city": city,
                         "fetch_time": fetch_time,
                         "raw_data": json.dumps(data)
                     })
        conn.commit()
    logging.info(f"Fetched raw data from {city}: {fetch_time}")

def transform_and_store():
    # Fetch all raw data for today.
    today = datetime.now().date()
    check_query = text("""
        SELECT 1
        FROM transformed_weather
        WHERE date = :today
        LIMIT 1
    """)
    with  engine.connect() as conn:
        exists = conn.execute(check_query, {"today": today}).fetchone()

        if exists:
            print(f"Transformation already completed for {today}")
            logging.error(f"Transformation already completed for {today}")
            return


    query = text("""
    SELECT
        city,
        raw_data->'main'->>'temp' AS temp,
        raw_data->'main'->>'humidity' AS humidity
    FROM raw_weather
    WHERE DATE(fetch_time) = :today
    """)
    df = pd.read_sql(query, engine, params={"today": today})
    df['temp'] = df['temp'].astype(float)
    df['humidity'] = df['humidity'].astype(float)
    df[['temp', 'humidity']] = df[['temp', 'humidity']].round(2)
    logging.info(f"Loaded to dataframe with {len(df)} rows")

    if not df.empty:
        averages = df.groupby('city').agg(
            {'temp':'mean',
             'humidity':'mean'}
        ).reset_index()
        averages['date'] = today

#         Store in transformed table(append, or upsert if needed)
        with engine.connect() as conn:
            for _, row in averages.iterrows():
                conn.execute(text("""
                INSERT INTO transformed_weather (city, date, avg_temp, avg_humidity)
                VALUES (:city, :date, :temp, :humidity)
                ON CONFLICT DO NOTHING 
                """), row.to_dict())
            conn.commit()
            logging.info("Inserted transformed weather data")

city = "Doha"
data = fetch_weather(city)
if data:
    store_raw_data(city, data)

transform_and_store()
print("ETL Completed for doha only")