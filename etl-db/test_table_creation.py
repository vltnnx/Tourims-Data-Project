from postgres_warehouse import create_tables, set_primary_keys
import pandas as pd

dim_continents = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_continents.csv")
dim_countries = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_countries.csv")
dim_cities = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_cities.csv")
dim_stations = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_stations.csv")
dim_datetime = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_datetime.csv")
fact_weather = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/fact_weather.csv")

# CREATE SQL TABLES
tables = [dim_continents, dim_countries, dim_cities, dim_stations, dim_datetime, fact_weather]
table_names = ["dim_continents", "dim_countries", "dim_cities", "dim_stations", "dim_datetime", "fact_weather"]
primary_keys = ["continent_id", "country_id", "city_id", "station_id", "datetime_id", "log_id"]

create_tables(tables, table_names)
set_primary_keys(table_names, primary_keys)