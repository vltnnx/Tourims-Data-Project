import pandas as pd

def load_continents(filepath):
    country_continents = pd.read_csv(filepath)
    country_continents.rename(columns={"Country":"country_name", "Continent":"continent_name"}, inplace=True)

    return country_continents