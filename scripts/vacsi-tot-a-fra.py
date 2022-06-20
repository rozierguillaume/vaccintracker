import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/dc103057-d933-4e4b-bdbf-36d312af9ca9"
  data = requests.get(url)

  with open('data/input/vacsi-tot-a-fra.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df):
  df=df[df["clage_vacsi"] != 0]
  df_clage_vacsi = pd.read_csv('data/input/clage_spf.csv', sep=';')
  df = df.merge(df_clage_vacsi, left_on="clage_vacsi", right_on="code_spf").groupby(["jour", "categorie-large"]).sum().reset_index()
  return df

def import_fra_data():
  df = pd.read_csv('data/input/vacsi-tot-a-fra.csv', sep=';')
  return prepare_data(df)

def csv_to_json_fra(df):
  jour_max = df.jour.max()
  df = df[(df.jour == jour_max)]
  df = df.sort_values(by="categorie-large")
  dict_json = {"date": jour_max,
              "age": df["categorie-large"].tolist(), 
              "n_tot_dose1": df.n_tot_dose1.tolist(), 
              "n_tot_complet": df.n_tot_complet.tolist(),
              "n_tot_rappel": df.n_tot_rappel.tolist(),
              "n_tot_2_rappel": df.n_tot_2_rappel.tolist(),
              "couv_tot_dose1": list(np.round(df.n_tot_dose1.values/df["pop"].values*100, 2)),
              "couv_tot_complet": list(np.round(df.n_tot_complet.values/df["pop"].values*100, 2)),
              "couv_tot_rappel": list(np.round(df.n_tot_rappel.values/df["pop"].values*100, 2)),
              "couv_tot_2_rappel": list(np.round(df.n_tot_2_rappel.values/df["pop"].values*100, 2)),
              "population": list(np.round(df["pop"].values))
              }

  with open("data/output/vacsi-tot-a-fra_lastday.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)
