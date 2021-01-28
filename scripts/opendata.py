import json
import pandas as pd
import requests

def download_france_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/efe23314-67c4-45d3-89a2-3faef82fae90"
  data = requests.get(url)

  with open('data/input/vacsi-fra.csv', 'wb') as f:
          f.write(data.content)

def import_france_data():
  df = pd.read_csv('data/input/vacsi-fra.csv', sep=';')
  return df

def csv_to_json(df):
  dict_json = {}
  dict_json["dates"] = df["jour"].tolist()
  dict_json["n_dose1"] = df["n_dose1"].tolist()

  with open("data/output/vacsi-fra.json", "w") as outfile: 
    outfile.write(json.dumps(dict_json))

download_france_data()
df = import_france_data()
csv_to_json(df)
