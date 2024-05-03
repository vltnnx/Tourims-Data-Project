
import psycopg2 as pg
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import db_key

connection = pg.connect(host="localhost", dbname="postgres", user="postgres", \
                        password=db_key, port=5432)
sql = 
cursor = connection.cursor()


def set_primary_key(table_names_list, primary_key_list):
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from config import db_key

    conn = pg.connect(host="localhost", dbname="postgres", user="postgres", \
                            password=db_key, port=5432)
    cur = conn.cursor()

    for table_name, primary_key in zip(table_names_list, primary_key_list):
        cur.execute("ALTER TABLE %s ADD PRIMARY KEY (%s);",
                     (table_name, primary_key)
        )

    conn.commit()
    cur.close()
    conn.close()


cursor.execute(CREATE TABLE IF NOT EXISTS person (
               id INT PRIMARY KEY,
               name VARCHAR(255),
               age INT,
               gender CHAR
)
)

cursor.execute("""COPY 
""")

# temp_avg,temp_min,temp_max,precipitation,snow_depth,wind_direction,wind_speed,wind_speed_gust,air_pressure,sunshine_min,station_id,datetime_id,log_id



connection.commit()

cursor.close()
connection.close()


"""
# Define your Python variables
city = 'New York'
population_threshold = 1000000

# Execute a parameterized SQL query using Python variables
cur.execute(
    "SELECT * FROM cities WHERE city = %s AND population > %s",
    (city, population_threshold)
)
"""