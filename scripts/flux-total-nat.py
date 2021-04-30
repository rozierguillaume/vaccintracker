import json
import pandas as pd
import requests
import numpy as np

types_vaccins_key = [1, 2, 3, 4]
types_vaccins_name = ["Pfizer", "Moderna", "AstraZeneca", "Janssen"]

def download_flux_total_nat():
  url = "https://www.data.gouv.fr/fr/datasets/r/9c60af86-b974-4dba-bf34-f52686c7ada9"
  data = requests.get(url)

  with open('data/input/flux-total-nat.csv', 'wb') as f:
          f.write(data.content)

def import_flux_total_nat():
  df = pd.read_csv('data/input/flux-total-nat.csv', sep=None)
  #df["date_fin_semaine"] = parsedate(df.date_fin_semaine.values)
  return df

def csv_to_json_flux_total_nat(df_tous, df):
  dict_json = {"noms_vaccins": types_vaccins_name, "types_vaccins": types_vaccins_key}
  dict_json["jour"] = df_tous.date_fin_semaine.fillna(0).to_list()
  dict_json["nb_doses_cum"] = df_tous.nb_doses_cum.fillna(0).to_list()

  for key in types_vaccins_key:
    dict_json_vaccin = {}
    df_vaccin = df[df.type_de_vaccin == types_vaccins_name[key-1]]
    df_vaccin = df_vaccin.merge(df_tous["date_fin_semaine"], left_on="date_fin_semaine", right_on="date_fin_semaine", how="right").groupby("date_fin_semaine").first().reset_index().sort_values(by="date_fin_semaine")
    df_vaccin = prepare_flux_total_nat_vaccin(df_vaccin)
    print(df_vaccin)
    dict_json_vaccin["jour"] = df_vaccin.date_fin_semaine.fillna(0).to_list()
    dict_json_vaccin["nb_doses_cum"] = df_vaccin.nb_doses_cum.fillna(0).to_list()
    
    dict_json[key] = dict_json_vaccin

  with open("data/output/flux-total-nat.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))

def prepare_flux_total_nat_vaccin(df):
  df["nb_doses_cum"] = df["nb_doses"].fillna(0).cumsum()
  return df

def prepare_flux_total_nat(df):
  df = df.groupby("date_fin_semaine").sum().reset_index()
  df["nb_doses_cum"] = df["nb_doses"].cumsum()
  return df

def import_data_flux_total_nat():
  df = pd.read_csv('data/input/flux-total-nat.csv', sep=None, engine='python')
  return df

def parsedate(dates):
  newdates = []
  for date in dates:
    if (("/" in date) & ("-" not in date)):
      split = date.split("/")
      if(len(split)>=3):
        newdates += [split[2] + "-" + split[1] + "-" + split[0]]
      else:
        newdates += [date]
    else:
        newdates += [date]
  return newdates

download_flux_total_nat()
df = import_data_flux_total_nat()
df_tous_types = prepare_flux_total_nat(df)
csv_to_json_flux_total_nat(df_tous_types, df)