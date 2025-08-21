import pandas as pd
from datetime import datetime
from Transform_data import df
import re

# Cleaning the data by stripping whitespace and capitalizing column names

def split_camel_case(col):
    return re.sub(r'(?<=[a-z])(?=[A-Z])', '_', col).capitalize()
df.columns = [split_camel_case(col) for col in df.columns]

df.columns = df.columns.str.strip().str.capitalize().str.replace(' ', '_')
df = df.rename(columns={'Pw_number': 'Project_worksheet(pw)_number'})
df = df.rename(columns={'Dcc': 'Donation_control_center_(DCC)'})

# Converting date columns to datetime format
cols_date = ['Declaration_date', 'Obligated_date', 'Last_refresh']
for col in cols_date:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col])



# Coverting amount to it right data type "float"
cols_amount  = ['Project_amount', 'Federal_share_obligated', 'Total_obligated', 'Mitigation_amount']
for amount_details in cols_amount:
    if amount_details in df.columns:
        df[amount_details] = df[amount_details].astype(float)

# Separting Declararion date obligated date and Last refresh into year and month
for date_details in cols_date:
    if date_details in df.columns:
        df[date_details + '_Year'] = df[date_details].dt.year
        df[date_details + '_Month'] = df[date_details].dt.strftime('%B')
        


# df.drop(columns=['Obligated_date', 'Declaration_date', 'Last_refresh'], inplace=True)
df.drop(columns=["Hash", "Id"], inplace=True)

df = pd.DataFrame(df)


#df.to_csv('cleaned_fema.csv', index=True)

#print(df.head())
#print(df.tail())
