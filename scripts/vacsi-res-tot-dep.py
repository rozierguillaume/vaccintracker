import pandas as pd
import requests
import json


def download_vacsi_tot_fra():
  url = "https://www.data.gouv.fr/fr/datasets/r/1cbfd282-e882-479d-afcc-958f09f617ec"
  data = requests.get(url)

  with open('data/input/vacsi-res-tot-dep.csv', 'wb') as f:
        f.write(data.content)

def prepare_data(df):
    return df

def import_vacsi_res_tot_dep():
    return prepare_data(pd.read_csv('data/input/vacsi-res-tot-dep.csv', sep=";"))

def export_json(df):
    deps = df["dep"].unique().tolist()
    dict_json = {}

    for dep in deps:
        df_dep = df[df.dep == dep]
        dict_json[dep] = {'res_couv_tot_dose1': list(df_dep['res_couv_tot_dose1'].fillna(0))[0], 'res_couv_tot_dose2': list(df_dep['res_couv_tot_dose2'].fillna(0))[0]}
        
    with open("data/output/vacsi-res-tot-dep.json", "w") as outfile:
        outfile.write(json.dumps(dict_json))


download_vacsi_tot_fra()
df = import_vacsi_res_tot_dep()
export_json(df)

