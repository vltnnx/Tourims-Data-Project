import pandas as pd

def load_cities(filepath, separator):
    df = pd.read_csv(filepath, sep=separator)
    coordinates = df["Coordinates"].str.split(", ", expand=True)
    city_df = pd.DataFrame()
    city_df["city_name"] = df["Name"]
    city_df["country_name"] = df["Country name EN"]
    city_df["latitude"] = coordinates[0]
    city_df["longitude"] = coordinates[1]

    return city_df

# test = load_cities()
# print(test)


# Name
# Country name EN
# Population (?)
# Coordinates