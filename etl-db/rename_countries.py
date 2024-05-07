import pandas as pd

def rename_countries(*dfs):
    """ Takes the DataFrames containing country names and renames
    the countries to standardize the names across all DataFrames.

    Notice: Renames countries in DataFrames within a "country_name"
    column.
    
    Parameters:
     - dfs : DataFrames containing country names.
    """
    edited_dfs = []

    for df in dfs:
        df.loc[df["country_name"] == "Cabo Verde", "country_name"] = "Cape Verde"
        df.loc[df["country_name"] == "North Macedonia", "country_name"] = "Macedonia"
        df.loc[df["country_name"] == "Macedonia, The former Yugoslav Rep. of", "country_name"] = "Macedonia"
        df.loc[df["country_name"] == "Türkiye", "country_name"] = "Turkey"
        df.loc[df["country_name"] == "Russian Federation", "country_name"] = "Russia"
        df.loc[df["country_name"] == "Moldova, Republic of", "country_name"] = "Moldova"
        df.loc[df["country_name"] == "Syrian Arab Republic", "country_name"] = "Syria"
        df.loc[df["country_name"] == "Iran, Islamic Republic of", "country_name"] = "Iran"
        df.loc[df["country_name"] == "Iran, Islamic Rep. of", "country_name"] = "Iran"
        df.loc[df["country_name"] == "Korea, Democratic People's Republic of", "country_name"] = "North Korea"
        df.loc[df["country_name"] == "Korea, Dem. People's Rep. of", "country_name"] = "North Korea"
        df.loc[df["country_name"] == "Korea, Republic of", "country_name"] = "South Korea"
        df.loc[df["country_name"] == "Burma (Myanmar)", "country_name"] = "Myanmar"
        df.loc[df["country_name"] == "Viet Nam", "country_name"] = "Vietnam"
        df.loc[df["country_name"] == "Lao People's Democratic Republic", "country_name"] = "Laos"
        df.loc[df["country_name"] == "Lao People's Dem. Rep.", "country_name"] = "Laos"
        df.loc[df["country_name"] == "Tanzania, United Republic of", "country_name"] = "Tanzania"
        df.loc[df["country_name"] == "Congo, The Democratic Republic of the", "country_name"] = "Congo (Kinshasa)"
        df.loc[df["country_name"] == "Democratic Republic of Congo", "country_name"] = "Congo (Kinshasa)"
        df.loc[df["country_name"] == "Congo, Democratic Republic of the", "country_name"] = "Congo (Kinshasa)"
        df.loc[df["country_name"] == "Congo", "country_name"] = "Congo (Brazzaville)"
        df.loc[df["country_name"] == "Burkina", "country_name"] = "Burkina Faso"
        df.loc[df["country_name"] == "Ivory Coast", "country_name"] = "Côte d'Ivoire"
        df.loc[df["country_name"] == "Venezuela, Bolivarian Republic of", "country_name"] = "Venezuela"
        df.loc[df["country_name"] == "Venezuela, Bolivarian Rep. of", "country_name"] = "Venezuela"
        df.loc[df["country_name"] == "Bolivia, Plurinational State of", "country_name"] = "Bolivia"
        df.loc[df["country_name"] == "Micronesia, Federated States of", "country_name"] = "Micronesia"
        df.loc[df["country_name"] == "Brunei Darussalam", "country_name"] = "Brunei"

        edited_dfs.append(df)

    return tuple(edited_dfs)

def remove_non_countries(*dfs):
    """ Removes territories that aren't countries, or
    for which there is no data in other required DataFrames.
    """
    edited_dfs = []

    for df in dfs:
        df = df[df["country_name"] != "Greenland"]
        df = df[df["country_name"] != "Gibraltar"]
        df = df[df["country_name"] != "Macao"]
        df = df[df["country_name"] != "Macau, China"]
        df = df[df["country_name"] != "Réunion"]
        df = df[df["country_name"] != "French Southern Territories"]
        df = df[df["country_name"] != "Saint Pierre and Miquelon"]
        df = df[df["country_name"] != "Bermuda"]
        df = df[df["country_name"] != "Cayman Islands"]
        df = df[df["country_name"] != "Virgin Islands, U.S."]
        df = df[df["country_name"] != "French Guiana"]
        df = df[df["country_name"] != "Antarctica"]
        df = df[df["country_name"] != "United States Minor Outlying Islands"]
        df = df[df["country_name"] != "Cook Islands"]
        df = df[df["country_name"] != "French Polynesia"]
        df = df[df["country_name"] != "Réunion"]
        df = df[df["country_name"] != "Western Sahara"]

        edited_dfs.append(df)

    return tuple(edited_dfs)