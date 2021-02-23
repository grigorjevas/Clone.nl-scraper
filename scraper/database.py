import config
from psycopg2 import connect, OperationalError


def connect_to_database():
    try:
        connection = connect(
            host=config.HOST,
            database=config.DB,
            user=config.USER,
            password=config.PASSWORD,
            port=config.PORT
        )
        print("Connecting to the db...")
        return connection
    except OperationalError as err:
        print("pg error: ", err.pgerror, "\n")
        print("pg code: ", err.pgcode, "\n")


def execute_query(query):
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()


def fetch_data(query):
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()
