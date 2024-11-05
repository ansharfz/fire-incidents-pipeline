import socket


def load_data(df):
    # Load data to Postgres DB
    postgres_ip = socket.gethostbyname('postgres-db')
    properties = {
        'user': 'user',
        'password': 'password',
        'driver': 'org.postgresql.Driver',
    }

    url = f'jdbc:postgresql://{postgres_ip}:5432/fire-incident-db'
    df.write.mode('overwrite').jdbc(url = url, table = 'fire_incident', properties = properties)
