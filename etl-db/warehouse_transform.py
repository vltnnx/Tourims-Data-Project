import pandas as pd

def continents_dimension(country_continents):
    unique_continents = country_continents.continent_name.unique()
    dim_continents = pd.DataFrame()
    dim_continents["continent_name"] = unique_continents
    dim_continents["continent_id"] = range(1, len(dim_continents) + 1)

    return dim_continents

def country_dimension(weather_stations, country_continents, dim_continents):
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
    dim_cities = pd.merge(city_df, dim_countries[["country_name", "country_id"]], on="country_name")
    dim_cities.drop(columns=["country_name"], inplace=True)
    dim_cities.sort_values(by=["city_name"], ascending=True, inplace=True)
    dim_cities["city_id"] = range(1, len(dim_cities) + 1)

    dim_cities = dim_cities[~dim_cities["city_name"].isnull()]

    return dim_cities

def station_dimension(stations_df, dim_countries):
    dim_station = pd.merge(stations_df, dim_countries[["country_name", "country_id"]], on="country_name")
    dim_station.drop(columns=["country_code", "country_name"], inplace=True)
    dim_station.rename(columns={"daily_start":"data_start", "daily_end":"data_end"}, inplace=True)

    return dim_station

def datetime_dimension(dim_station):
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

# weather = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/staging_weather.csv")
# datetime = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/dim_datetime.csv")
# datetime["date"] = datetime["date"].astype("datetime64[s]")

# weather_table = weather_fact(weather, datetime)

# # print(weather_table.head(20))
# print(weather.info())
# print(datetime.info())