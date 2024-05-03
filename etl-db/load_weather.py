from datetime import datetime
from meteostat import Stations, Daily
import pandas as pd


"""
TO-DO

 - Format numbers (desired number of decimals)
 - Check & change country names if

"""



def get_country_weather(weather_stations):
    countries = weather_stations.country_name.unique()
    weather_data = weather_by_country(countries, weather_stations)

    return weather_data

def weather_by_country(countries, weather_stations):
    all_countries = pd.DataFrame()
    data_start, data_end = determine_dates()

    num_countries = len(countries)
    num_loaded = 0

    for country in countries:
        country_stations = weather_stations[weather_stations["country_name"] == country].copy()

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

    return all_countries

def format_country_weather(df, country_name):
    def month_name(date):
        month = date.strftime("%B")
        return month
    
    df["month_name"] = df["time"].apply(month_name)
    # df.drop(columns=["time"], inplace=True)
    # group = df.groupby("month_name", observed=True).mean()
    # group["country_name"] = country_name
    # group.reset_index(inplace=True)

    # return group
    # # WITHOUT GROUPING
    df["country_name"] = country_name
    return df
       
def get_station_weather(id, data_start, data_end):
    data = Daily(id, data_start, data_end)
    data = data.fetch()

    return data

def determine_dates():
    end_year = datetime.now().year - 1
    start_year = end_year - 4
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)

    return start, end


# get_country_weather()
# determine_dates()
