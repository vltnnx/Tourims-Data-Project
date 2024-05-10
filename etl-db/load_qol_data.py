import pandas as pd
import os
import datetime

RAW_DATA_PATH = os.getcwd() + "/data/raw/"
COST_OF_LIVING_DATA = RAW_DATA_PATH + "cost_of_living.csv"
HEALTH_CARE_DATA = RAW_DATA_PATH + "health_care.csv"
POLLUTION_DATA = RAW_DATA_PATH + "pollution.csv"
SAFETY_DATA = RAW_DATA_PATH + "safety.csv"

CLEAN_DATA_PATH = os.getcwd() + "/data/clean-db/"

def load_qol_data():
    health_df = load_health()
    pollution_df = load_pollution()
    safety_df = load_safety()
    cost_df = load_cost()

    health_df, pollution_df, safety_df, cost_df = \
        create_country_col(health_df, pollution_df, safety_df, cost_df)
    
    health_df, pollution_df, safety_df, cost_df = \
        unpivot(health_df, pollution_df, safety_df, cost_df)
    
    health_df, pollution_df, safety_df, cost_df = \
        rename_columns(health_df, pollution_df, safety_df, cost_df)
    
    qol_df = concat_qol_dfs(health_df, pollution_df, safety_df, cost_df)
    qol_df.rename(columns={"country":"country_name", "city":"city_name"}, inplace=True)
    qol_df = rename_drop_countries(qol_df)
    qol_df = rename_drop_cities(qol_df)
    qol_df["date"] = qol_df["date"].astype("datetime64[s]")

    return qol_df
    return health_df, pollution_df, safety_df, cost_df

def load_health():
    """ Loads health care data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(HEALTH_CARE_DATA)
    df = df[["City", "Health Care Index"]]
    df = df.sort_values(by="Health Care Index", ascending=False)
    df["Health Care Rank"] = range(1, len(df) + 1)

    data_date = os.path.getctime(HEALTH_CARE_DATA)
    data_date = datetime.datetime.fromtimestamp(data_date).date()
    data_date = data_date.strftime("%Y-%m-%d")
    df["Date"] = data_date

    return df

def load_pollution():
    """ Loads pollution data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(POLLUTION_DATA)
    df = df[["City",  "Pollution Index"]]
    df = df.sort_values(by="Pollution Index", ascending=False)
    df["Pollution Rank"] = range(1, len(df) + 1)

    data_date = os.path.getctime(POLLUTION_DATA)
    data_date = datetime.datetime.fromtimestamp(data_date).date()
    data_date = data_date.strftime("%Y-%m-%d")
    df["Date"] = data_date

    return df

def load_safety():
    """ Loads safety data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(SAFETY_DATA)
    df = df[["City",  "Safety Index"]]
    df = df.sort_values(by="Safety Index", ascending=False)
    df["Safety Rank"] = range(1, len(df) + 1)

    data_date = os.path.getctime(SAFETY_DATA)
    data_date = datetime.datetime.fromtimestamp(data_date).date()
    data_date = data_date.strftime("%Y-%m-%d")
    df["Date"] = data_date

    return df

def load_cost():
    """ Loads cost of living data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(COST_OF_LIVING_DATA)
    df = df[["City",  "Cost of Living Index", "Groceries Index", "Restaurant Price Index"]]
    df = df.sort_values(by="Cost of Living Index", ascending=False)
    df["Cost of Living Rank"] = range(1, len(df) + 1)

    data_date = os.path.getctime(COST_OF_LIVING_DATA)
    data_date = datetime.datetime.fromtimestamp(data_date).date()
    data_date = data_date.strftime("%Y-%m-%d")
    df["Date"] = data_date

    return df

def create_country_col(*dfs):
    formatted_dfs = []

    for df in dfs:
        city_country = df["City"].str.split(", ", expand=True, n=1)
        df["City"] = city_country[0]
        df["Country"] = city_country[1]
        df["Country"] = df["Country"].apply(lambda x: x.split(", ")[1] if ", United States" in x else x)
        df["Country"] = df["Country"].apply(lambda x: x.split(", ")[1] if ", Georgia" in x else x)


        formatted_dfs.append(df)

    return tuple(formatted_dfs)

def unpivot(*dfs):
    unpivoted_dfs = []

    for df in dfs:
        melted = df.melt(id_vars=["Date", "City", "Country"], var_name="indicator", value_name="value")

        unpivoted_dfs.append(melted)

    return tuple(unpivoted_dfs)

def rename_columns(*dfs):
    formatted_dfs = []

    for df in dfs:
        col_names = df.columns
        new_col_names = []

        for col_name in col_names:
            col_name = col_name.lower()
            col_name = col_name.replace(" ", "_")
            new_col_names.append(col_name)

        for idx in range(len(new_col_names)):
            curr_col_name = df.columns[idx]
            new_col_name = new_col_names[idx]

            df.rename(columns={curr_col_name:new_col_name}, inplace=True)

        formatted_dfs.append(df)

    return tuple(formatted_dfs)

# def create_country_indexes(*dfs):
    country_dfs = []

    for df in dfs:
        index_col = df.columns[1]
        rank_col = f"{index_col[0:-6]}_rank"
        country_df = df.groupby("country")[index_col].mean().reset_index()
        country_df = country_df.sort_values(by=index_col, ascending=False)
        country_df[index_col] = country_df[index_col].round(1)
        country_df[rank_col] = range(1, len(country_df) + 1)

        country_dfs.append(country_df)

    return tuple(country_dfs)

# def calculate_country_index(*dfs):
    country_dfs = []

    for df in dfs:
        pass

# def merge_index_dfs(*dfs):
    index_df = dfs[0]
    key_col = dfs[0].columns[0]

    for df in dfs[1:]:
        if df.columns[-1] == "country":
            df.drop(columns=["country"], inplace=True)
            index_df = pd.merge(index_df, df, on=key_col, how="outer")
        else:
            index_df = pd.merge(index_df, df, on=key_col, how="outer")

    return index_df

def concat_qol_dfs(*dfs):
    index_df = pd.DataFrame()

    for df in dfs:
        index_df = pd.concat([index_df, df], ignore_index=True)

    return index_df

def rename_drop_countries(df):
    df = df[df["country_name"] != "Taiwan"]
    df = df[df["country_name"] != "Belgium"]
    df = df[df["country_name"] != "Singapore"]
    df = df[df["country_name"] != "Puerto Rico"]
    df = df[df["country_name"] != "Bermuda"]
    df = df[df["country_name"] != "Montenegro"]
    df = df[df["country_name"] != "Guatemala"]
    df = df[df["country_name"] != "Kosovo (Disputed Territory)"]

    df.loc[df["country_name"] == "Czech Republic", "country_name"] = "Czechia"
    df.loc[df["country_name"] == "Bosnia And Herzegovina", "country_name"] = "Bosnia and Herzegovina"
    df.loc[df["country_name"] == "North Macedonia", "country_name"] = "Macedonia"
    df.loc[df["country_name"] == "Hong Kong (China)", "country_name"] = "China"
    df.loc[df["country_name"] == "Shandong, China", "country_name"] = "China"
    df.loc[df["country_name"] == "BC, Canada", "country_name"] = "Canada"
    df.loc[df["country_name"] == "Trinidad And Tobago", "country_name"] = "Trinidad and Tobago"

    return df

def rename_drop_cities(df):
    rename_list = [
        ["Ad Dammam", "Dammam"],
        ["Isfahan (Esfahan)", "Isfahan"],
        ["Asuncion", "Asunción"],
        ["Rostov-na-donu", "Rostov-na-Donu"],
        ["Sao Jose dos Campos", "São José dos Campos"],
        ["Nis", "Niš"],
        ["Rzeszow", "Rzeszów"],
        ["Kosice", "Košice"],
        ["Cordoba", "Córdoba"],
        ["Heraklion", "Iráklion"],
        ["Palma de Mallorca", "Palma"],
        ["Aarhus", "Århus"],
        ["Dusseldorf", "Düsseldorf"],
        ["Malmo", "Malmö"],
        ["Cancun", "Cancún"],
        ["Hanover", "Hannover"],
        ["Cologne", "Köln"],
        ["Seville (Sevilla)", "Sevilla"],
        ["Ajman", "Ajman City"],
        ["Ras al-Khaimah", "Ras Al Khaimah City"],
        ["Gdansk", "Gdańsk"],
        ["Poznan", "Poznań"],
        ["Cebu", "Cebu City"],
        ["Marrakech", "Marrakesh"],
        ["Bhopal", "Bhopāl"],
        ["Kiev (Kyiv)", "Kyiv"],
        ["Thessaloniki", "Thessaloníki"],
        ["Wroclaw", "Wrocław"],
        ["Brasilia", "Brasília"],
        ["Odessa (Odesa)", "Odesa"],
        ["The Hague (Den Haag)", "The Hague"],
        ["Medellin", "Medellín"],
        ["Frankfurt", "Frankfurt am Main"],
        ["Tel Aviv-Yafo", "Tel Aviv"],
        ["Nizhny Novgorod", "Nizhniy Novgorod"],
        ["Geneva", "Les Geneveys-sur-Coffrane"],
        ["Malaga", "Málaga"],
        ["Quebec City", "Québec"],
        ["Zurich", "Zürich"],
        ["Reykjavik", "Reykjavík"],
        ["Gothenburg", "Göteborg"],
        ["Astana (Nur-Sultan)", "Astana"],
        ["Hyderabad", "Hyderābād"],
        ["Bogota", "Bogotá"],
        ["New York", "New York City"],
        ["Montreal", "Montréal"],
        ["San Jose", "San José"],
        ["Jeddah (Jiddah)", "Jeddah"],
        ["Sao Paulo", "São Paulo"],
        ["Krakow (Cracow)", "Kraków"]
    ]

    drop_list = [
    "Tripoli",
    "Erbil (Irbil)",
    "Plzen",
    "Ostrava",
    "Almere",
    "Freiburg im Breisgau",
    "Sudbury",
    "Constanta",
    "Florianopolis",
    "St.Catharines",
    "Lodz",
    "Makati",
    "Queretaro (Santiago de Querétaro)",
    "Nuremberg",
    "Penang",
    "Eskisehir",
    "Merida",
    "Brasov",
    "Chandigarh",
    "Goa",
    "Iasi",
    "Dehradun",
    "Bali",
    "Lucknow (Lakhnau)",
    "Brno",
    "Prague",
    "Izmir",
    "Timisoara",
    "Hong Kong",
    "Bangalore",
    "Kochi",
    "Ulaanbaatar",
    "Ghaziabad",
    "Ludhiana",
    "San Jose"
]

    for city_name in drop_list:
        df = df[df["city_name"] != city_name]

    for old_new in rename_list:
        df.loc[df["city_name"] == str(old_new[0]), "city_name"] = str(old_new[1])

    df.reset_index(inplace=True, drop=True)

    return df



# health, pollution, safety, cost = load_qol_data()
# index_combined = concat_qol_dfs(health, pollution, safety, cost)
# quality_of_life = load_qol_data()
# print(quality_of_life)
# quality_of_life.to_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean-db/fact_quality.csv", index=False)


# city_indexes = merge_index_dfs(health, pollution, safety, cost)
# country_indexes = merge_index_dfs(health_country, pollution_country, safety_country, cost_country)

# print(country_indexes)
# print(city_indexes)






# import os
# import datetime
# COST_OF_LIVING_DATA = RAW_DATA_PATH + "cost_of_living.csv"
# HEALTH_CARE_DATA = RAW_DATA_PATH + "health_care.csv"
# POLLUTION_DATA = RAW_DATA_PATH + "pollution.csv"
# SAFETY_DATA = RAW_DATA_PATH + "safety.csv"

# file_paths = [COST_OF_LIVING_DATA, HEALTH_CARE_DATA, POLLUTION_DATA, SAFETY_DATA]
# file_dates = []

# for path in file_paths:
#     creation_time = os.path.getctime(path)
#     creation_date = datetime.datetime.fromtimestamp(creation_time).date()
#     file_dates.append(creation_date)

# oldest_date = min(file_dates)
# oldest_date_formatted = oldest_date.strftime("%Y-%m-%d")

# print("Oldest Date:", oldest_date_formatted)

