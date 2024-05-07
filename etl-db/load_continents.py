import pandas as pd

def load_continents(filepath):
    """ Loads the file containing countries and continents into a DataFrame.
    
    1. Renames "Country" and "Continent" columns to match other DataFrames'
       column names as "country_name", "continent_name" respectively.
    2. Returns "country_continents" DataFrame.

    Parameters:
     - filepath : File path to raw data file containing countries and their
                  continents.
    """
    country_continents = pd.read_csv(filepath)
    country_continents.rename(columns={"Country":"country_name", "Continent":"continent_name"}, inplace=True)

    return country_continents