import pandas as pd

# Load the specific sheet "Domestic Tourism-Accommodation" from the Excel file
file_path = '/Users/joonas/VSCode/Tourims Data Project/data/unwto-all-data-download_112023.xlsx'

df = pd.read_excel(file_path, engine='openpyxl', sheet_name="Domestic Tourism-Accommodation", header=None)

# Drop 3 first columns
df.drop(df.columns[[0,1,2]], axis=1, inplace=True)

# Drop 2 first rows
df = df.iloc[2:]

# Format country name column
df.iloc[:, 0] = df.iloc[:, 0].ffill()
df.rename(columns={df.columns[0]:"Country"}, inplace=True)

# Format category column
category = df.iloc[2, 1]
df.rename(columns={df.columns[2]:category}, inplace=True)
df.iloc[:, 2] = df.iloc[:, 2].ffill()

# Format units & category description columns
df = df.rename(columns={df.columns[5]:"Units"})
df = df.rename(columns={df.columns[3]:"Description"})

# Drop unnecessary columns
df.drop(df.columns[[1,4,6]], axis=1, inplace=True)

# Drop rows based on empty data rows
df.dropna(subset=[df.columns[4]], inplace=True)

# Rename year columns
shape = range(4,df.shape[1])
for i in range(4,df.shape[1]):
    df.rename(columns={df.columns[i]:df.iloc[0, i]}, inplace=True)

# Drop first row and last column (unnecessary)
df.drop(df.index[0], inplace=True)
df.drop(columns=df.columns[-1], inplace=True)

# Capitalize values in column 0
df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: x.title() if isinstance(x, str) else x)


# # df.to_csv("testdf.csv")

print(df.head(20))