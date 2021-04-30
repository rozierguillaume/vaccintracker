import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/b273cf3b-e9de-437c-af55-eda5979e92fc"
  data = requests.get(url)

  with open('data/input/vacsi-v-fra.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df):
  df=df[df["vaccin"] != 0]
  return df

def import_fra_data():
  df = pd.read_csv('data/input/vacsi-v-fra.csv', sep=';')
  return prepare_data(df)

def csv_to_json_fra(df):
  types_vaccins = [1, 2, 3, 4]
  noms_vaccins = ["Pfizer/BioNTech", "Moderna", "AstraZeneca", "Janssen"]
  dict_json = {"types_vaccins": types_vaccins, "noms_vaccins": noms_vaccins}

  for type_vaccin in types_vaccins:
    df_temp = df[df.vaccin == type_vaccin]
    dict_json[type_vaccin] = {
      "jour" : list(df.jour.unique()),
      "n_dose1": [0] * (abs(len(df_temp)-len(df.jour.unique()))) + list(df_temp.n_dose1),
      "n_cum_dose1": [0] * (abs(len(df_temp)-len(df.jour.unique()))) + list(df_temp.n_cum_dose1),
      "n_dose2": [0] * (abs(len(df_temp)-len(df.jour.unique()))) + list(df_temp.n_dose2),
      "n_cum_dose2": [0] * (abs(len(df_temp)-len(df.jour.unique()))) + list(df_temp.n_cum_dose2)
    }

  with open("data/output/vacsi-v-fra.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))

def csv_to_json_fra_tous_types(df):
  df = df.groupby("jour").sum().reset_index()

  dict_json = {
    "jour" : list(df.jour.unique()),
    "n_dose1": list(df.n_dose1),
    "n_dose1_rolling": list(df.n_dose1.rolling(window=7, center=True).mean().fillna(0)),
    "n_cum_dose1": list(df.n_cum_dose1),
    "n_dose2": list(df.n_dose2),
    "n_dose2_rolling": list(df.n_dose2.rolling(window=7, center=True).mean().fillna(0)),
    "n_cum_dose2": list(df.n_cum_dose2),
    "n_dose_tot.rolling": list((df.n_dose1+df.n_dose2).rolling(window=7, center=True).mean().fillna(0))
  }

  with open("data/output/vacsi-ndose-fra.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)
csv_to_json_fra_tous_types(df)