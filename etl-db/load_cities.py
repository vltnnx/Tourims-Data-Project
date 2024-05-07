import pandas as pd

def load_cities(filepath, separator):
    """ Loads the raw data file containing cities.

    1. Splits the "Coordinates" column into a separate "latitude" and
       "longitude" columns.
    2. Creates a "city_df" DataFrame with columns "city_name" (from
       "Name" column), "country_name" (from "Country name EN" column), 
       "latitude", and "longitude".

    Returns "city_df" DataFrame.

    Parameters:
     - filepath : File path to raw file containing city and country names,
                  and city coordinates.
     - separator : Determine separator for .csv file (";" in project data file).
    
    """
    df = pd.read_csv(filepath, sep=separator)
    coordinates = df["Coordinates"].str.split(", ", expand=True)
    city_df = pd.DataFrame()
    city_df["city_name"] = df["Name"]
    city_df["country_name"] = df["Country name EN"]
    city_df["latitude"] = coordinates[0]
    city_df["longitude"] = coordinates[1]

    return city_df