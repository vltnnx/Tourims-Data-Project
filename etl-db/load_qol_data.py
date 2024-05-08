import pandas as pd
import os

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
        format_data(health_df, pollution_df, safety_df, cost_df)
    
    return health_df, pollution_df, safety_df, cost_df

def load_health():
    """ Loads health care data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(HEALTH_CARE_DATA)
    df = df[["City", "Health Care Index"]]
    df = df.sort_values(by="Health Care Index", ascending=False)
    df["health_care_rank"] = range(1, len(df) + 1)

    return df

def load_pollution():
    """ Loads pollution data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(POLLUTION_DATA)
    df = df[["City",  "Pollution Index"]]
    df = df.sort_values(by="Pollution Index", ascending=False)
    df["pollution_rank"] = range(1, len(df) + 1)

    return df

def load_safety():
    """ Loads safety data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(SAFETY_DATA)
    df = df[["City",  "Safety Index"]]
    df = df.sort_values(by="Safety Index", ascending=False)
    df["safety_rank"] = range(1, len(df) + 1)

    return df

def load_cost():
    """ Loads cost of living data .csv and selects relevant columns. 
    
    Returns a DataFrame.
    """
    df = pd.read_csv(COST_OF_LIVING_DATA)
    df = df[["City",  "Cost of Living Index", "Groceries Index", "Restaurant Price Index"]]
    df = df.sort_values(by="Cost of Living Index", ascending=False)
    df["cost_of_living_rank"] = range(1, len(df) + 1)

    return df

def format_data(*dfs):
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

        city_country = df["city"].str.split(", ", expand=True, n=1)
        df["city"] = city_country[0]
        df["country"] = city_country[1]
        df["country"] = df["country"].apply(lambda x: x.split(", ")[1] if ", United States" in x else x)

        formatted_dfs.append(df)

    return tuple(formatted_dfs)

def country_indexes(*dfs):
    country_dfs = []

    for df in dfs:
        index_col = df.columns[1]
        rank_col = f"{index_col[0:-6]}_rank"
        country_df = df.groupby("country")[index_col].mean().reset_index()
        country_df = country_df.sort_values(by=index_col, ascending=False)
        country_df[index_col] = country_df[index_col].round(1)
        country_df[rank_col] = range(1, len(country_df) + 1)

        print(index_col)

        country_dfs.append(country_df)

    return tuple(country_dfs)


health, pollution, safety, cost = load_qol_data()

health_country, pollution_country, safety_country, cost_country = \
    country_indexes(health, pollution, safety, cost)

print(health_country)

# print(health_country[health_country["country"] == "United States"])

# str = "test_string_column"
# print(f"{str[0:-7]}_rank")


# print(COST_OF_LIVING_DATA)