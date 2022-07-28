import sqlalchemy
import sqlite3

DATABASE_LOCATION = "sqlite:///weather_data.sqlite"


# ------------
# This function is used to store the data
# ------------
def data_load(data):

    # Creating the engine
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('weather_data.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS weather(
            daytime VARCHAR(200),
            day VARCHAR(200),
            time VARCHAR(200),
            temperature VARCHAR(200),
            isDay VARCHAR(200),
            humidity VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (daytime)
        )
    """

    cursor.execute(sql_query)

    print("Database open successfully\n")

    try:
        data.to_sql("weather", engine, index=False, if_exists='append')
    except:
        print("Database has the data.\n")

    conn.close()

    print("Database close successfully.\n")

