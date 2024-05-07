from datetime import datetime
from meteostat import Stations, Daily
import pandas as pd
import os

CURR_DIR_PATH = os.getcwd()
DATA_PATH = CURR_DIR_PATH + "/data/clean-db/"
STAGING_WEATHER_PATH = DATA_PATH + "staging_weather.csv"

def get_country_weather(weather_stations):
    """ Creates a list of unique countries within the "weather_data" DataFrame.
    Calls the "weather_by_country()" function to request weather data for selected
    weather stations for each country.

    Returns a DataFrame that contains 5 year weather data for 1-5 weather
    stations in each country.

    Parameters:
     - weather_stations : DataFrame containing all weather stations to request
                          data for.
    """
    countries = weather_stations.country_name.unique()
    weather_data = weather_by_country(countries, weather_stations)

    return weather_data

def weather_by_country(countries, weather_stations):
    """ Iterates over all unique countries and the selected weather stations in
    those to request 5-year weather data through meteostat library, appending 
    the data into a single DataFrame "all_countries".

    If weather data for a country already exists (in "staging_weather.csv"), takes 
    the existing data for the country instead of requesting it through meteostat
    library.

    Prints out function progress throughout the iterations.

    Saves (overwrites) the "staging_weather.csv" file with updated data where
    applicable and returns "all_countries" DataFrame, containing weather data
    for all selected weather stations.

    Parameters:
     - countries : List of (unique) country names to loop through.
     - weather_stations : DataFrame of weather stations containing meteostat
                          weather station id for the weather request.
    
    """
    all_countries = pd.DataFrame()
    data_start, data_end = determine_dates()

    num_countries = len(countries)
    num_loaded = 0

    try:
        staging_weather = pd.read_csv(STAGING_WEATHER_PATH, low_memory=False)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        staging_weather = pd.DataFrame()

    for country in countries:
        country_stations = weather_stations[weather_stations["country_name"] == country].copy()

        exists = weather_exists(staging_weather, country, data_end)

        if exists == False:
            print(f"Weather for {country} not in database, loading data . . .")

            country_weather = pd.DataFrame()

            for row in range(len(country_stations)):
                id = country_stations.iloc[row, 0]
                station_weather = get_station_weather(id, data_start, data_end)
                station_weather["station_id"] = id
                station_weather.reset_index(inplace=True)
                country_weather = pd.concat([country_weather, station_weather], ignore_index=True)

            country_weather = format_country_weather(country_weather, country)
            all_countries = pd.concat([all_countries, country_weather], ignore_index=True)
            
            num_loaded += 1
            percent_loaded = round((num_loaded / num_countries) * 100, 2)
            print(f"{num_loaded} out of {num_countries} countries loaded ({percent_loaded}%).\nLatest country: {country}.", "\n")

        else:
            print(f"Weather for {country} already in database.")
            country_weather = staging_weather[staging_weather["country_name"] == country]
            all_countries = pd.concat([all_countries, country_weather], ignore_index=True)

            num_loaded += 1
            percent_loaded = round((num_loaded / num_countries) * 100, 2)
            print(f"{num_loaded} out of {num_countries} countries loaded ({percent_loaded}%).\nLatest country: {country}.", "\n")


    all_countries.to_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/staging_weather.csv", index=False)

    return all_countries

def format_country_weather(df, country_name):
    """ Adds a month name column into the DataFrame.
    """
    def month_name(date):
        month = date.strftime("%B")
        return month
    
    df["month_name"] = df["time"].apply(month_name)
    df["country_name"] = country_name

    return df
       
def get_station_weather(id, data_start, data_end):
    """ Uses meteostat library's Daily class to retrieve daily weather data
    for a weather station based on meteostat station id, start date, and end date.

    Parameters:
     - id : Meteostat weather station id
     - data_start : Date from which weather data should be requested for
     - data_end : Date until which weather data should be requested for
    
     Returns daily weather data in a DataFrame format for a single weather station.
    """
    data = Daily(id, data_start, data_end)
    data = data.fetch()

    return data

def determine_dates():
    """ Calculates the timespan for which weather data should be 
    retrieved for.

    Function is configured to calculate the timespan by taking the last
    day of previous year as the end date, and first day of the year
    5 years before that.

    Returns a start date and end date, resulting in a 5-year timespan
    to retrieve weather data for.
    """
    end_year = datetime.now().year - 1
    start_year = end_year - 4
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)

    return start, end

def weather_exists(staging_weather, country, data_end):
    """ Checks if weather data for a country already exists in the
    "staging_weather.csv".

    If no data exists (weather_by_country() function reads an empty .csv
    or file doesn't exist) returns "False" to trigger requesting weather data
    for that country.

    If country exists and the data end date in "staging_weather.csv" is up until
    the end of last year, returns "True" triggering weather_by_country() function
    to keep existing data for that country.

    If either the country doesn't exist or the data isn't up until the last day
    of the previous year in the "staging_weather.csv", also returns "False"
    to trigger weather_by_country() function to request weather data for
    that country.

    Parameters:
     - staging_weather : DataFrame containing weather data for selected countries &
                         weather stations.
     - country : Country name to check against staging_weather DataFrame.
     - data_end : Date up until which the data should exist.
    
    """
    if staging_weather.shape == (0, 0):
        return False
    else:
        staging_weather["time"] = staging_weather["time"].astype(str)
        data_end = str(data_end)[:10]

        if country in staging_weather["country_name"].values and data_end in staging_weather["time"].values:
            return True
        else:
            return False