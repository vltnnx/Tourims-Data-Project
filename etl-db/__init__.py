import os
from load_weather_stations import load_stations_file
from load_weather import get_country_weather
from load_cities import load_cities
from load_continents import load_continents
from rename_countries import remove_non_countries, rename_countries
from warehouse_transform import *
from postgres_warehouse import create_tables, set_primary_keys
from load_qol_data import load_qol_data

CURR_DIR_PATH = os.getcwd()
RAW_DATA_PATH = CURR_DIR_PATH + "/data/raw/"
CLEAN_DATA_PATH = CURR_DIR_PATH + "/data/clean-db/"
# SOURCE DATA
WEATHER_STATIONS_RAW = RAW_DATA_PATH + "stations_meteostat.json"
CITIES_RAW = RAW_DATA_PATH + "geonames-all-cities-with-a-population-1000.csv"
CONTINENTS_RAW = RAW_DATA_PATH + "Countries by continents.csv"
# DATA DESTINATION (CLEAN DATA)
DIM_CONTINENTS_PATH = CLEAN_DATA_PATH + "dim_continents.csv"
DIM_COUNTRIES_PATH = CLEAN_DATA_PATH + "dim_countries.csv"
DIM_CITIES_PATH = CLEAN_DATA_PATH + "dim_cities.csv"
DIM_STATIONS_PATH = CLEAN_DATA_PATH + "dim_stations.csv"
DIM_DATETIME_PATH = CLEAN_DATA_PATH + "dim_datetime.csv"
FACT_WEATHER_PATH = CLEAN_DATA_PATH + "fact_weather.csv"

# LOAD RAW DATA
weather_stations, load_weather = load_stations_file(WEATHER_STATIONS_RAW)
cities = load_cities(CITIES_RAW, separator=";")
country_continents = load_continents(CONTINENTS_RAW)
country_weather = get_country_weather(load_weather)
qol_data = load_qol_data()


# TRANSFORM DATA
weather_stations, cities, country_continents, country_weather = \
    remove_non_countries(weather_stations, cities, country_continents, country_weather)
weather_stations, cities, country_continents, country_weather = \
    rename_countries(weather_stations, cities, country_continents, country_weather)

# CREATE DIMENSIONS
dim_continents = continents_dimension(country_continents)
dim_countries = country_dimension(weather_stations, country_continents, dim_continents)
dim_cities = city_dimension(cities, dim_countries)
dim_stations = station_dimension(weather_stations, dim_countries)
dim_datetime = datetime_dimension(dim_stations)
fact_weather = weather_fact(country_weather, dim_datetime)
dim_quality_indicators = quality_indicators_dim(qol_data)
fact_quality = quality_fact(qol_data, dim_countries, dim_cities, dim_datetime, dim_quality_indicators)

# # CREATE SQL TABLES
# tables = [dim_continents, dim_countries, dim_cities, dim_stations, dim_datetime, fact_weather]
# table_names = ["dim_continents", "dim_countries", "dim_cities", "dim_stations", "dim_datetime", "fact_weather"]
# primary_keys = ["continent_id", "country_id", "city_id", "station_id", "datetime_id", "log_id"]

# create_tables(tables, table_names)
# set_primary_keys(table_names, primary_keys)


# SAVE DIMENSIONS (OPTIONAL)
# dim_continents.to_csv(DIM_CONTINENTS_PATH, index=False, encoding="utf-8", line_terminator='\n')
# dim_countries.to_csv(DIM_COUNTRIES_PATH, index=False, encoding="utf-8")
# dim_cities.to_csv(DIM_CITIES_PATH, index=False, encoding="utf-8")
# dim_stations.to_csv(DIM_STATIONS_PATH, index=False, encoding="utf-8")
# dim_datetime.to_csv(DIM_DATETIME_PATH, index=False, encoding="utf-8")
# fact_weather.to_csv(FACT_WEATHER_PATH, index=False, encoding="utf-8")
fact_weather.to_csv(f"{CLEAN_DATA_PATH}fact_quality.csv", index=False, encoding="utf-8")
dim_quality_indicators.to_csv(f"{CLEAN_DATA_PATH}dim_quality_indicators.csv", index=False, encoding="utf-8")