from ExtractData import data_extract
from TransformData import data_transform
from LoadData import data_load


# ------------
# Weather ETL Pipeline
# ------------
def run_weather_etl():
    print("\n")
    print("ETL Pipeline\n")

    print("Extraction stage started...\n")

    # Data variable get the weather data in a json format
    data = data_extract()

    print("Transform stage started...\n")

    # Data variable is sent to be modified
    data = data_transform(data)

    print("Load stage started...\n")

    # Data variable is storage
    data_load(data)


if __name__ == '__main__':
    run_weather_etl()

