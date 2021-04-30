import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/6820ff9f-2dbb-4e87-8565-fcd7fa2dfa0f"
  data = requests.get(url)

  with open('data/input/livraison-national.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df):
  df=df.groupby(["date"]).sum().reset_index()
  df_clage_vacsi = pd.read_csv('data/input/livraison-anciennes.csv', sep=';')
  #df = df.merge(df_clage_vacsi, left_on="clage_vacsi", right_on="code_spf").groupby(["jour", "categorie-large"]).sum().reset_index()
  return df

def import_fra_data():
  df = pd.read_csv('data/input/livraison-national.csv', sep=',')
  return prepare_data(df)

def csv_to_json_fra(df):
  

  with open("data/output/livraison-national.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)
