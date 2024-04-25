import pandas as pd
import numpy as np

df = pd.read_csv("/Users/joonas/VSCode/Tourims Data Project/data/clean/Domestic Tourism-Accommodation.csv")

# Pivot the table
df_pivot = df.pivot_table(index=['Country', 'Year'], columns='Domestic Tourism-Accommodation', values='Value', aggfunc='first')

# Reset index to flatten the DataFrame
df_pivot.reset_index(inplace=True)

print(df_pivot.head(50))

# print(df.head(20))

# print(df["Units"][0])