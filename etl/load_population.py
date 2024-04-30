import os
import pandas as pd

CURR_DIR_PATH = os.getcwd()
RAW_DATA_PATH = CURR_DIR_PATH + "/data/raw/"
CLEAN_DATA_PATH = CURR_DIR_PATH + "/data/clean/"
WEATHER_STATIONS_PATH = CLEAN_DATA_PATH + "weather_stations_country.csv"
POPULATION_DATA_RAW = RAW_DATA_PATH + "geonames-all-cities-with-a-population-1000.csv"

def load_population():
    df = pd.read_csv(POPULATION_DATA_RAW, sep=";")

    population_df = pd.DataFrame()
    population_df["city"] = df["Name"]
    population_df["country_name"] = df["Country name EN"]
    population_df["population"] = df["Population"]
    population_df["coordinates"] = df["Coordinates"]

    return population_df

def select_cities():
    countries_stations = countries_from_db()
    population_df = load_population()
    countries_population = population_df.country_name.unique()

    selected_cities = pd.DataFrame()
    station_country_not_found = []

    for country in countries_stations:
        if country in countries_population:
            country_cities = population_df[population_df["country_name"] == country]
            country_cities = country_cities.sort_values(by=["population"], ascending=False)
            country_cities = country_cities.head(10)
            selected_cities = pd.concat([selected_cities, country_cities], ignore_index=True)
        else:
            station_country_not_found.append(country)

    selected_cities.to_csv(f"{CLEAN_DATA_PATH}largest_cities.csv")
    station_country_not_found = pd.DataFrame(station_country_not_found)
    station_country_not_found.to_csv(f"{CLEAN_DATA_PATH}station_country_not_found.csv")

def countries_from_db():
    weather_stations = pd.read_csv(WEATHER_STATIONS_PATH)
    countries = weather_stations.country_name.unique()

    return countries



select_cities()