import pandas as pd
import os
import numpy as np

CURR_DIR_PATH = os.getcwd()
RAW_DATA_PATH = os.path.join(CURR_DIR_PATH, "data/raw/")
CLEAN_DATA_PATH = os.path.join(CURR_DIR_PATH, "data/clean/")

def format_worksheet(filename):
    document = pd.ExcelFile(RAW_DATA_PATH + filename)
    worksheets = document.sheet_names
    extract_sheets = worksheets[1:]

    for worksheet in extract_sheets:
        save_as = worksheet.strip()

        df = pd.read_excel(RAW_DATA_PATH + filename, engine='openpyxl', sheet_name=worksheet, header=None)

        # Drop 3 first columns
        df.drop(df.columns[[0,1,2]], axis=1, inplace=True)

        # Drop 2 first rows
        df = df.iloc[2:]

        if df.iloc[0, 6] != "1995":
            df.drop(df.columns[6], axis=1, inplace=True)

        # Format country name column
        df.iloc[:, 0] = df.iloc[:, 0].ffill()
        df.rename(columns={df.columns[0]:"Country"}, inplace=True)

        # Format category column
        category = save_as        
        df.rename(columns={df.columns[2]:category}, inplace=True)

        # Format units & category description columns
        df = df.rename(columns={df.columns[5]:"Units"})
        df = df.rename(columns={df.columns[3]:"Description"})

        # Format sheets with total values
        sheets_with_total = ["Inbound Tourism-Arrivals", "Inbound Tourism-Regions", "Inbound Tourism-Purpose", \
                             "Inbound Tourism-Transport", "Inbound Tourism-Expenditure", "Domestic Tourism-Trips", \
                             "Outbound Tourism-Departures", "Outbound Tourism-Expenditure", "Employment"]
        
        if save_as in sheets_with_total:
            description_distinct = df["Description"].dropna().unique().tolist()

            for i in range(len(description_distinct)):
                rename = description_distinct[i]

                df.loc[df["Description"] == rename, save_as] = df["Description"]
        
        # Format sheets without total values
        else:
            df.iloc[:, 2] = df.iloc[:, 2].ffill()

        # Drop unnecessary columns
        df.drop(df.columns[[1,4,6]], axis=1, inplace=True)

        # Drop rows based on empty data rows
        df.dropna(subset=[df.columns[4]], inplace=True)

        # Rename year columns
        for i in range(4,df.shape[1]):
            df.rename(columns={df.columns[i]:df.iloc[0, i]}, inplace=True)

        # Drop first row and last column (unnecessary)
        df.drop(df.index[0], inplace=True)
        df.drop(columns=df.columns[-1], inplace=True)

        # Drop rows with empty values in column 1
        df = df.dropna(subset=[df.columns[1]])

        # Capitalize values in column 0
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: x.title() if isinstance(x, str) else x)

        df = unpivot_year(df)

        # Dropping "Description" column if descriptions in column with total
        if save_as in sheets_with_total:
            df.drop(columns=df.columns[2], inplace=True)

        df = df.dropna(subset=[df.columns[0]])

        df = format_numbers(df)
        df = combine_descriptive_columns(df, save_as)
        df = pivot_descriptive_columns(df, save_as)

        save_csv(save_as, df)
        # # df.to_csv("testdf.csv")

        # print(df.head(20))

def save_csv(sheet_name, df):
    csv = sheet_name + ".csv"
    save_as = os.path.join(CLEAN_DATA_PATH, csv)
    df.to_csv(save_as, index=False)

def unpivot_year(df):
    columns = list(df.columns[0:4])
    unpivoted_df = pd.melt(df, id_vars=columns, var_name="Year", value_name="Value")

    return unpivoted_df

def replace_null(df):
    # Replace null values in column index 2 with values from column index 1
    df.iloc[:, 2].fillna(df.iloc[:, 1], inplace=True)
    
    # Rename column index 2 to the name of column index 1
    df.rename(columns={df.columns[2]: df.columns[1]}, inplace=True)
    
    # Drop column index 1
    df.drop(df.columns[1], axis=1, inplace=True)

def format_numbers(df):
    df.loc[df["Value"] == "..", "Value"] = np.nan
    df["Value"] = df["Value"].astype(str).str.replace('\xa0', '')
    df["Value"] = df["Value"].astype(float)
    df["Value"] = df["Value"].fillna(0)
    df["Value"] = df["Value"].astype(int)
    df.loc[df["Value"] == 0, "Value"] = np.nan

    if df["Units"].iloc[0] == "Thousands":
        df["Value"] = df["Value"].multiply(1000)

    elif df["Units"].iloc[0] == "US$ Millions":
        df["Value"] = df["Value"].multiply(1000000)

    else:
        pass

    return df

def combine_descriptive_columns(df, save_as):
    if save_as == "Tourism Industries":
        df.drop(columns=["Description"], inplace=True)
    
    elif "Description" in df.columns:
        df[save_as] = df[save_as] + ", " + df["Description"]
        df.drop(columns=["Description"], inplace=True)

    else:
        pass

    if df["Units"][0] == "Thousands":
        df.loc[df["Units"] == "Thousands", "Units"] = np.nan

    elif df["Units"][0] == "US$ Millions":
        df.loc[df["Units"] == "US$ Millions", "Units"] = np.nan

    else: 
        pass

    return df

def pivot_descriptive_columns(df, save_as):
    df_pivot = df.pivot_table(index=['Country', 'Year'], columns=save_as, values='Value', aggfunc='first').reset_index()

    return df_pivot







format_worksheet("unwto-all-data-download_112023.xlsx")

# df = pd.read_csv("data/clean/Domestic Tourism-Accommodation.csv")
# columns = list(df.columns[0:4])
# unpivoted_df = pd.melt(df, id_vars=columns, var_name="Year", value_name="Value")

# print(unpivoted_df.head(50))