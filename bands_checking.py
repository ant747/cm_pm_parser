import sys
import json
import pandas as pd


def count_band(data):
    with open('bands.json', mode='r', encoding='utf-8') as file:
        lte_data = json.load(file)["lte"]
    band_res = None
    for num in lte_data:
        if lte_data[num]["min"] <= int(data) <= lte_data[num]["max"]:
            band_res = lte_data[num]["band"]
            break
    return int(band_res) if band_res else None


def get_band(file_name):
    data = pd.read_csv(file_name)
    df = pd.DataFrame(data)

    filtered_df = df[df['attribute'].isin(['earfcndl', 'earfcnul'])]

    values = filtered_df['value']

    bands = []
    for value in values:
        band = count_band(value)
        bands.append(band)

    df.loc[df['attribute'].isin(['earfcndl', 'earfcnul']), 'band'] = bands

    df.to_csv(f'{file_name}NEW.csv', index=False)
    return df


file_name = sys.argv[1]
# get_band(file_name)
data = pd.read_csv(file_name)
df = pd.DataFrame(data)

df = get_band(file_name)

e_df = df.loc[df['attribute'].isin(['earfcndl', 'earfcnul']), ['path_to_object', 'value', 'band']]
no_e_df = df.loc[~df['attribute'].isin(['earfcndl', 'earfcnul']), ['path_to_object', 'ManagedElement', 'cell',
                                                                    'object_type', 'object_id', 'attribute', 'value']]
e_df.rename(columns={"value": "earfcn"}, inplace=True)
#e_df.to_csv('E_DF.csv', index=False)
#no_e_df.to_csv('NO_E_DF.csv', index=False)

result = no_e_df.merge(e_df, on='path_to_object', how='left')
# result.rename(columns={'value': 'earfcn'}, inplace=True)
# result['value'] = result['value'].fillna(e_df['value'])
# result['band'] = result['band'].fillna(e_df['band'])
new_file = f'{file_name}.bands.csv'
result.to_csv(new_file, index=False)
