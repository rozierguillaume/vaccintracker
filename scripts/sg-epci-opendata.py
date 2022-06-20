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
  df["jour"]=df["semaine_glissante"].map(lambda x: x[11:]) # Semaine glissante to jour}
  return df

def import_fra_data():
  df = pd.read_csv('data/input/sg-epci-opendata.csv', sep=',')
  print(df.ti_classe.unique())
  return prepare_data(df)

def import_old_fra_data():
  with open('data/output/sg-epci.json') as f:
    data = json.load(f)
  return data

def csv_to_json_fra(df, old_dict):
  dict_json = old_dict
  liste_epci = df.filter(regex=("epci20.*")).iloc[:,0].unique()
  dates = sorted(df.jour.unique())[-50:]
  dict_json["dates"] = sorted(list(set(dict_json["dates"]+dates)))

  for date in dates:
    print(date)
    dict_json[date] = {}
    df_temp = df[df["jour"] == date] # Keep last day

    for epci in liste_epci:
      dict_json[date][str(epci)] = df_temp[df_temp.filter(regex=("epci20.*")).iloc[:,0] == epci]["ti_classe"].fillna("0").values[0]

  with open("data/output/sg-epci.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_fra_data()
old_dict = import_old_fra_data()
df = import_fra_data()
csv_to_json_fra(df, old_dict)
