import pandas as pd
import pycountry
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import numpy as np
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data.raw.country_bounding import bounding_boxes

CURR_DIR_PATH = os.getcwd()
RAW_DATA_PATH = CURR_DIR_PATH + "/data/raw/"
CLEAN_DATA_PATH = CURR_DIR_PATH + "data/clean/"



def load_stations_file(filename):
    file = RAW_DATA_PATH + filename
    df = pd.read_json(file)

    parsed = pd.DataFrame({
                'id': df['id'],
                'name_en': df['name'].apply(lambda x: x.get('en', None)),
                'country': df['country'],
                'latitude': df['location'].apply(lambda x: x.get('latitude', None)),
                'longitude': df['location'].apply(lambda x: x.get('longitude', None)),
                'timezone': df['timezone'],
                'daily_start': df['inventory'].apply(lambda x: x['daily'].get('start', None)),
                'daily_end': df['inventory'].apply(lambda x: x['daily'].get('end', None)),
            })
    
    parsed["country_name"] = parsed["country"].apply(get_country_name)

    parsed = clean(parsed)

    weather_stations = select_stations(parsed)

    print(weather_stations)

def get_country_name(code):
    try:
        country = pycountry.countries.get(alpha_2=code)
        return country.name
    except AttributeError:
        return "Unknown"
    
def clean(df):
    # Remove rows with "Unknown"
    df = df[df["country_name"] != "Unknown"]
    # Remove rows with NULL in daily_start
    df = df[(~df["daily_start"].isnull())]
    # Create new index
    df = df.reset_index(drop=True)
    # Transform dates to datetime format
    df['daily_start'] = pd.to_datetime(df['daily_start'])
    df['daily_end'] = pd.to_datetime(df['daily_end'])
    # Take only weather stations with valid timespans for data
    df = df[(df["daily_start"].dt.year <= 2018) & (df["daily_end"].dt.year == 2024)]
    df = df.reset_index(drop=True)
    # Rename columns
    df.rename(columns= {"name_en":"weather_station", "country":"country_code"}, inplace=True)

    return df

def select_stations(df):
    countries = df.country_name.unique()

    num_clusters = 5
    weather_stations = pd.DataFrame()

    for country in countries:
        country_df = df[df["country_name"] == country].copy()

        if country_df.shape[0] <= 5:
            # If the country has 5 or fewer weather stations, add them directly
            weather_stations = pd.concat([weather_stations, country_df], ignore_index=True)
        else:
            if country in bounding_boxes:
                # If the country has a bounding box defined
                min_lon, min_lat, max_lon, max_lat = bounding_boxes[country]
                country_df = country_df[(country_df["latitude"] >= min_lat) & (country_df["latitude"] <= max_lat) &
                                        (country_df["longitude"] >= min_lon) & (country_df["longitude"] <= max_lon)].copy()

            # Apply clustering to the filtered country dataframe
            kmeans = KMeans(n_clusters=num_clusters)
            country_df["cluster"] = kmeans.fit_predict(country_df[["latitude", "longitude"]])

            selected_stations = pd.DataFrame()

            for cluster_id in range(num_clusters):
                cluster_stations = country_df[country_df["cluster"] == cluster_id]

                centroid = np.mean(cluster_stations[["latitude", "longitude"]], axis=0)

                def closest_station_to_centroid(cluster):
                    distances = np.linalg.norm(cluster[["latitude", "longitude"]] - centroid, axis=1)
                    closest_station_index = np.argmin(distances)
                    return cluster.iloc[closest_station_index]

                selected_station = cluster_stations.groupby("cluster").apply(closest_station_to_centroid, include_groups=False)
                selected_stations = pd.concat([selected_stations, selected_station], ignore_index=True)

            weather_stations = pd.concat([weather_stations, selected_stations], ignore_index=True)

    return weather_stations

    
load_stations_file("stations_meteostat.json")

