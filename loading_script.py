from sqlalchemy import create_engine
import pandas as pd

# Connect to PostgreSQL database in Docker container
engine = create_engine('postgresql://user:password \
                       @localhost:5432/fire-incident-db')

# Load data from a CSV file
data = pd.read_csv('Fire_Incidents_20240516.csv')

# Write data to PostgreSQL database
data.head()
