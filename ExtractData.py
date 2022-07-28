import requests
from datetime import date
import datetime

# Token
TOKEN = "6090c434cedb4722960192453222607"


# ------------
# This function is used to get the data from the api
# ------------
def data_extract():

    # Variables to get the weather data from yesterday
    today = date.today()
    yesterday = today - datetime.timedelta(days=1)

    # Url of the website where we get the api
    url = f"http://api.weatherapi.com/v1/history.json?key={TOKEN}&q=Morovis&dt={yesterday}"

    request = requests.get(url)

    weather_data = request.json()

    return weather_data

