import json
import pandas as pd
import requests

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/efe23314-67c4-45d3-89a2-3faef82fae90"
  data = requests.get(url)

  with open('data/input/vacsi-fra.csv', 'wb') as f:
          f.write(data.content)

def download_reg_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/735b0df8-51b4-4dd2-8a2d-8e46d77d60d8"
  data = requests.get(url)

  with open('data/input/vacsi-reg.csv', 'wb') as f:
          f.write(data.content)

def csv_to_json_fra(df):
  dict_json = {}
  dict_json["dates"] = df["jour"].tolist()
  dict_json["n_dose1"] = df["n_dose1"].tolist()

  with open("data/output/vacsi-fra.json", "w") as outfile: 
    outfile.write(json.dumps(dict_json))

## REGIONS
def import_fra_data():
  df = pd.read_csv('data/input/vacsi-fra.csv', sep=';')
  return df

def import_reg_data():
  df = pd.read_csv('data/input/vacsi-reg.csv', sep=';')
  return df

def csv_to_json_reg(df):
  list_json = []

  for i in range(len(df)):
    dict_json = {}
    dict_json["date"] = df.loc[i,"jour"]
    dict_json["code"] = "REG-" + str(df.loc[i,"reg"])
    dict_json["n_dose1"] = int(df.loc[i, "n_dose1"])
    dict_json["n_cum_dose1"] = int(df.loc[i, "n_cum_dose1"])

    list_json += [dict_json]

  with open("data/output/vacsi-reg.json", "w") as outfile: 
    outfile.write(json.dumps(list_json, indent=4))



download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)

download_reg_data()
df = import_reg_data()
csv_to_json_reg(df)
