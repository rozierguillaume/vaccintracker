import pandas as pd
import requests
import json

#https://www.data.gouv.fr/fr/datasets/r/131c6b39-51b5-40a7-beaa-0eafc4b88466

def download_vacsi_tot_fra():
  url = "https://www.data.gouv.fr/fr/datasets/r/131c6b39-51b5-40a7-beaa-0eafc4b88466"
  data = requests.get(url)

  with open('data/input/vacsi-tot-fra.csv', 'wb') as f:
        f.write(data.content)

def import_vacsi_tot_fra():
    return pd.read_csv('data/input/vacsi-tot-fra.csv', sep=";")

def import_vacsi_fra():
    return pd.read_csv('data/input/vacsi-fra.csv', sep=";")

def import_last_output_data():
    with open('data/output/vacsi-fra-2doses.json') as f:
        data = json.load(f)
    return data

def merge_files(df, dict_data):
    jour_max = df.jour.max()

    if jour_max not in dict_data["jour"]:
        dict_data["jour"] += [jour_max]
        dict_data["n_dose2_cumsum"] += [list(df.n_tot_dose2)[-1]]

        dict_data["n_dose2"] = list(pd.Series(dict_data["n_dose2_cumsum"]).diff().fillna(0).astype(int))
        dict_data["n_dose2"][0] = dict_data["n_dose2_cumsum"][0]

    return dict_data

def export_json(dict_data):
    with open("data/output/vacsi-fra-2doses.json", "w") as outfile:
        outfile.write(json.dumps(dict_data))

def export_json_doses(dict_data):
    with open("data/output/somme-doses-rolling.json", "w") as outfile:
        outfile.write(json.dumps(dict_data))

def rolling_mean_doses(df_fra, dict_data):
    #df_2nd = pd.DataFrame()
    #df_2nd["jour"] = dict_data["jour"]
    #df_2nd["n_dose2"] = dict_data["n_dose2"]
    
    #df = df_fra.merge(df_2nd, left_on="jour", right_on="jour", how="outer").fillna(0)
    df_fra["rolling_doses"] = (df_fra.n_dose1 + df_fra.n_dose2).rolling(window=7, center=True).mean().round()
    return {"jour": list(df_fra.dropna().jour), "n_dose_rolling": list(df_fra.dropna().rolling_doses)}


download_vacsi_tot_fra()
df = import_vacsi_tot_fra()
dict_data = import_last_output_data()
merged_file = merge_files(df, dict_data)
export_json(merged_file)

df_fra = import_vacsi_fra()
dict_data_doses = rolling_mean_doses(df_fra, dict_data)
export_json_doses(dict_data_doses)

