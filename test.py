import requests
from datetime import datetime
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

eng = os.getenv('DB_URL')
engine = create_engine(eng)


def pgconn():
    conn = create_engine(eng)
    return conn

print(pgconn())
