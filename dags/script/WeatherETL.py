import requests
from datetime import date
import datetime

import pandas as pd

import sqlalchemy
import sqlite3


# ------------
# This function is used to verified if the data is correct
# ------------
def verified_data(df: pd.DataFrame) -> bool:

    # Verified if dataframe is empty
    if df.empty:
        print("Dataframe is empty.")
        return False

    # Verified Primary key
    if pd.Series(df["Daytime"]).is_unique:
        pass
    else:
        raise Exception("Primary key error.")

    # Verified if there is any null
    if df.isnull().values.any():
        raise Exception("Null value or values found.")

    # # Verified if data is from yesterday
    day = date.today() - datetime.timedelta(days=1)

    yesterday = str(day)

    days = df["Day"].tolist()

    for day in days:
        if day != yesterday:
            raise Exception("One or more data are not from yesterday.")

    return True


# ------------
# Weather ETL Pipeline
# ------------
def run_weather_etl():

    # ----------------------------------------------
    # Extract data
    # ----------------------------------------------

    # Token
    token = ""

    print("\n")
    print("ETL Pipeline\n")

    print("Extraction stage started...\n")

    # Variables to get the weather data from yesterday
    today = date.today()
    yesterday = today - datetime.timedelta(days=1)

    # Url of the website where we get the api
    url = f"http://api.weatherapi.com/v1/history.json?key={token}&q=Morovis&dt={yesterday}"

    print("url done")
    request = requests.get(url)

    # Data variable get the weather data in a json format
    weather_data = request.json()

    edata = weather_data

    # ----------------------------------------------
    # Transform data
    # ----------------------------------------------

    print("Transform stage started...\n")

    # Lists to store specific data
    daytime = []
    days = []
    hours = []
    temperature = []
    is_day = []
    humidity = []

    # Loop to assign the data to each list
    for hour_data in edata["forecast"]["forecastday"]:
        for data in hour_data["hour"]:
            daytime.append(data["time"])
            temperature.append(data["temp_f"])
            is_day.append(data["is_day"])
            humidity.append(data["humidity"])

    # Dictionary to store the data from the lists
    weather_dict = {
        "daytime": daytime,
        "day": days,
        "hour": hours,
        "temperature": temperature,
        "is_day": is_day,
        "humidity": humidity
    }

    # This list store all the values from the key "date"
    date_list = weather_dict["daytime"]

    # Initialization of lists
    days = []
    hours = []

    # Loop use to split the date from the hours
    for item in date_list:
        value = item.split(" ")
        days.append(value[0])
        hours.append(value[1])

    # Values from the list stored in their corresponding key
    weather_dict["day"] = days
    weather_dict["hour"] = hours

    # Converting the weather_dict to a dataframe
    weather_df = pd.DataFrame(weather_dict)

    # Lists that store the titles for each column of the dataframe
    columns_title = ["Daytime", "Day", "Hours", "Temperature", "isDay", "Humidity"]

    # Assign titles to each column of the dataframe
    weather_df.columns = columns_title

    #print(weather_df)
    print("\n")

    if verified_data(weather_df):
        print("Data is valid, proceed to load stage.\n")


    # ----------------------------------------------
    # Load data
    # ----------------------------------------------

    print("Load stage started...\n")

    db_location = "sqlite:///weatherdata.sqlite"

    # Creating the engine
    engine = sqlalchemy.create_engine(db_location)
    conn = sqlite3.connect('weatherdata.sqlite')
    cursor = conn.cursor()

    sql_query = """
            CREATE TABLE IF NOT EXISTS weather(
                Daytime VARCHAR(200),
                Day VARCHAR(200),
                Hours VARCHAR(200),
                Temperature VARCHAR(200),
                isDay VARCHAR(200),
                Humidity VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (Daytime)
            )
        """

    cursor.execute(sql_query)

    print("Database open successfully\n")
    weather_df.to_sql("weather", engine, index=False, if_exists='append')
    try:
        #weather_df.to_sql("weather", engine, index=False, if_exists='append')
        print("inside")
    except:
        print("Database has the data.\n")

    conn.close()

    print("Database close successfully.\n")
