import sys
from psycopg2 import connect, OperationalError


def connect_to_database():
    try:
        connection = connect(
            host="ec2-54-216-185-51.eu-west-1.compute.amazonaws.com",
            database="d8lj1gc9foqv0k",
            user="kprzglcktxqeai",
            password="c9db97b11715d85b4f03f5ff2ce2d3e247cbed18305fcc4808e42fa0c97eb8fd",
            port="5432"
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
