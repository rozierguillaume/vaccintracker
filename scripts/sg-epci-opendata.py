import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/34dcc90c-aec9-48ee-9fd3-a972b44202c0"
  data = requests.get(url)

  with open('data/input/sg-epci-opendata.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df):
  df=df[df["clage_65"] == 0] # Ne conserver que les lignes "tous âges"
  df["jour"]=df["semaine_glissante"].map(lambda x: x[11:]) # Semaine glissante to jour
  df[df["jour"] == df["jour"].max()] # Keep last day
  return df

def import_fra_data():
  df = pd.read_csv('data/input/sg-epci-opendata.csv', sep=',')
  return prepare_data(df)

def csv_to_json_fra(df):
  liste_epci = df["epci2020"].unique()
  dict_json = {}

  for epci in liste_epci:
    dict_json[str(epci)] = df[df["epci2020"] == epci]["ti_classe"].values[0]

  with open("data/output/sg-epci.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)
