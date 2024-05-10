import pandas as pd

def continents_dimension(country_continents):
    """ Creates a "dim_continents" dimension table.

    1. Reads unique continent names from "continent_name" column
    2. Creates a "dim_continent" DataFrame with a column "continent_name"
       out of the unique continents.
    3. Generates a "continent_id" column with a unique id for each continent
    4. Returns "dim_continents" DataFrame
    
    Parameters:
     - country_continents: DataFrame containing continent_name column"""
    unique_continents = country_continents.continent_name.unique()
    dim_continents = pd.DataFrame()
    dim_continents["continent_name"] = unique_continents
    dim_continents["continent_id"] = range(1, len(dim_continents) + 1)

    return dim_continents

def country_dimension(weather_stations, country_continents, dim_continents):
    """Creates the country dimension table.

    1. Reads unique country names from a "country_name" column in 
       weather_stations DataFrame.
    2. Creates a "dim_countries" DataFrame with a column "country_name"
       out of the unique countries.
    3. Sorts the values in alphabetical order and generates a "country_id"
       column with a unique id for each country
    4. Merges "continent_name" column from "country_continents" DataFrame
       to allow merging "continent_id" from "dim_continent"
    5. Drops "continent_name" column before returning "dim_countries"
    
    Parameters:
     - weather_stations: DataFrame containing all weather stations with country names
     - country_continents: DataFrame containing countries & continents
     - dim_continents: DataFrame created by "continent_dimension" function
    """
    unique_countries = weather_stations.country_name.unique()
    dim_countries = pd.DataFrame()
    dim_countries["country_name"] = unique_countries
    dim_countries.sort_values(by=["country_name"], ascending=True, inplace=True)
    dim_countries["country_id"] = range(1, len(dim_countries) + 1)

    dim_countries = pd.merge(dim_countries, country_continents[["country_name", "continent_name"]], on="country_name")
    dim_countries = pd.merge(dim_countries, dim_continents[["continent_name", "continent_id"]], on="continent_name")
    dim_countries.drop(columns=["continent_name"], inplace=True)

    return dim_countries

def city_dimension(city_df, dim_countries):
    """ Creates the city dimension table.

    1. Creates "dim_city" DataFrame by merging a DataFrame containing city_name 
       and country_name with dim_countries to add "country_id" column. Drops 
       "country_name" column.
    2. Sorts values in alphabetical order by "city_name" column and creates
       a unique id for each city into "city_id" column. Removes cities with
       null values in "city_name" column.
    3. Returns "dim_cities" DataFrame.
    
    Parameters:
     - city_df : DataFrame containing city & country names
     - dim_countries : DataFrame containing country name & id
    
    """
    dim_cities = pd.merge(city_df, dim_countries[["country_name", "country_id"]], on="country_name")
    dim_cities.drop(columns=["country_name"], inplace=True)
    dim_cities.sort_values(by=["city_name"], ascending=True, inplace=True)
    dim_cities["city_id"] = range(1, len(dim_cities) + 1)

    dim_cities = dim_cities[~dim_cities["city_name"].isnull()]

    return dim_cities

def station_dimension(stations_df, dim_countries):
    """ Creates dim_stations dimension DataFrame.

    1. Creates dim_station DataFrame by merging stations DataFrame containing
       country names for each station with "dim_countries", adding "country_id" column
       from "dim_countries" DataFrame.
    2. Drops "country_code" and "country_name" columns.
    3. More aptly names start and end date columns containing start and end date
       for daily weather data for each weather station.
    4. Returns "dim_station" DataFrame.

    Parameters:
     - stations_df : DataFrame containing required data for weather stations, including
                     "country_names" column.
     - dim_countries : DataFrame containing "country_id" & "country_name" columns.
    
    """
    dim_station = pd.merge(stations_df, dim_countries[["country_name", "country_id"]], on="country_name")
    dim_station.drop(columns=["country_code", "country_name"], inplace=True)
    dim_station.rename(columns={"daily_start":"data_start", "daily_end":"data_end"}, inplace=True)

    return dim_station

def datetime_dimension(dim_station):
    """ Creates dim_datetime dimension DataFrame.

    1. Takes the first existing date from dim_station, generating
       a date range from that date to "2030-12-31" (date determined
       in the date_range function below).
    2. Creates columns "year", "month" and "day_of_year".
    3. Creates a "datetime_id" with a unique id for each date.
    4. Returns "dim_datetime" dimension DataFrame.

    Parameters:
     - dim_station : DataFrame containing dates for existing weather data.
    
    """
    dim_station.sort_values(by="data_start", ascending=True, inplace=True)
    first_date = dim_station["data_start"][0]

    dates = pd.date_range(start=first_date, end="2030-12-31", freq="1D")

    dim_datetime = pd.DataFrame()
    dim_datetime["date"] = dates
    dim_datetime["date"] = dim_datetime["date"].astype("datetime64[s]")
    dim_datetime["year"] = dim_datetime["date"].dt.year
    dim_datetime["month"] = dim_datetime["date"].dt.month
    dim_datetime["day_of_year"] = dim_datetime["date"].dt.day_of_year

    dim_datetime.sort_values(by="date", ascending=True, inplace=True)
    dim_datetime["datetime_id"] = range(1, len(dim_datetime) + 1)

    return dim_datetime

def weather_fact(weather_df, dim_datetime):
    """ Creates fact_weather fact DataFrame.

    1. Creates the fact table by merging DataFrame containing weather data and dates
       with a DataFrame "dim_datetime" to allow adding "datetime_id" column.
    2. Drops columns "month_name", "country_name", and "date".
    3. Renames various indicator columns more aptly.
    4. Sorts column by "datetime_id" column and generates a "log_id" column
       for each logged row of weather data.
    """
    weather_df.rename(columns={"time":"date"}, inplace=True)
    weather_df["date"] = weather_df["date"].astype("datetime64[s]")

    fact_weather = pd.merge(weather_df, dim_datetime[["date", "datetime_id"]], on="date")

    fact_weather.drop(columns=["month_name", "country_name", "date"], inplace=True)
    fact_weather.rename(columns={"tavg":"temp_avg", "tmin":"temp_min", "tmax":"temp_max", "prcp":"precipitation", "snow":"snow_depth", \
                                 "wdir":"wind_direction", "wspd":"wind_speed", "wpgt":"wind_speed_gust", "pres":"air_pressure", \
                                 "tsun":"sunshine_min"}, inplace=True)
    
    fact_weather.sort_values(by="datetime_id", ascending=True, inplace=True)
    fact_weather["log_id"] = range(1, len(fact_weather) + 1)

    return fact_weather

def quality_fact(qol_df, dim_countries, dim_cities, dim_datetime, dim_quality_indicators):
   fact_quality = pd.merge(qol_df, dim_countries[["country_name", "country_id"]], on="country_name", how="left")
    
   # Filter out duplicated city-country combinations from dim_cities
   duplicate_city_country = dim_cities[["city_name", "country_id"]].duplicated()
   merge_cities = dim_cities[~duplicate_city_country]

   fact_quality = pd.merge(fact_quality, merge_cities[["city_name", "country_id", "city_id"]], on=["city_name", "country_id"])

   fact_quality = pd.merge(fact_quality, dim_datetime[["date", "datetime_id"]], on=["date"], how="left")
   fact_quality.drop(columns="date", inplace=True)

   fact_quality = pd.merge(fact_quality, dim_quality_indicators, on=["indicator"], how="left")
   fact_quality.drop(columns="indicator", inplace=True)

   fact_quality["log_id"] = range(1, len(fact_quality) + 1)

   return fact_quality

def quality_indicators_dim(df):
   indicators = df["indicator"].unique()

   dim_indicators = pd.DataFrame()
   dim_indicators["indicator"] = indicators
   dim_indicators["indicator_id"] = range(1, len(dim_indicators) + 1)

   return dim_indicators
    





    