import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/54dd5f8d-1e2e-4ccb-8fb8-eac68245befd"
  data = requests.get(url)

  with open('data/input/vacsi-a-fra.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df):
  df=df[df["clage_vacsi"] != 0]
  df_clage_vacsi = pd.read_csv('data/input/clage_spf.csv', sep=';')
  df = df.merge(df_clage_vacsi, left_on="clage_vacsi", right_on="code_spf").groupby(["jour", "categorie_fine"]).sum().reset_index()
  return df

def import_fra_data():
  df = pd.read_csv('data/input/vacsi-a-fra.csv', sep=None, engine='python')
  return prepare_data(df)

def csv_to_json_fra(df):
  #jour_max = df.jour.max()
  #df = df[(df.jour == jour_max)]
  #df = df.sort_values(by="categorie_fine")
  dict_json = {}

  for clage in df["categorie_fine"].tolist():
    df_clage = df[df["categorie_fine"] == clage]
    dict_json[clage] = {
                "jour": df_clage.jour.tolist(),
                "n_dose1": df_clage.n_dose1.tolist(), 
                "n_dose1_cum_pop": list(np.round(df_clage.n_cum_dose1.values/df_clage.population.values*100, 2))
              }

  with open("data/output/vacsi-tot-a-fra.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)
