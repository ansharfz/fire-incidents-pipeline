import socket


def load_data(df):
    # Load data to Postgres DB
    postgres_ip = socket.gethostbyname('postgres-db')
    properties = {
        'user': 'user',
        'password': 'password',
        'driver': 'org.postgresql.Driver',
        'mode': 'overwrite'
    }

    url = f'jdbc:postgresql://{postgres_ip}:5432/fire-incident-db'
    df.write.jdbc(url = url, table = 'fire_incident', properties = properties)
