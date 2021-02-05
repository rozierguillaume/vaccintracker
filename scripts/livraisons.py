import json
import pandas as pd
import requests
import numpy as np

## FRANCE
def download_data():
  #Flux a
  url = "https://www.data.gouv.fr/fr/datasets/r/6c39a8be-cd65-465a-a656-f8c645c839ac"
  data = requests.get(url)

  with open('data/input/flux-a-pfizer-nat.csv', 'wb') as f:
          f.write(data.content)

  #Flux b
  url = "https://www.data.gouv.fr/fr/datasets/r/f93be527-044b-4f33-baea-5e1c2f22e8aa"
  data = requests.get(url)

  with open('data/input/flux-b-pfizer-nat.csv', 'wb') as f:
          f.write(data.content)
  
  #Moderna
  url = "https://www.data.gouv.fr/fr/datasets/r/fb7ae18e-d49f-4008-9baf-f9af35152544"
  data = requests.get(url)

  with open('data/input/flux-moderna-nat.csv', 'wb') as f:
          f.write(data.content)
          
def prepare_data(df_flux_a_pfizer, df_flux_b_pfizer, df_flux_moderna):
  date_str = "date_debut_semaine"

  df_flux_a_pfizer=df_flux_a_pfizer.rename({"nb_doses": "nb_doses_flux_a_pfizer"}, axis=1)
  df_flux_b_pfizer=df_flux_b_pfizer.rename({"nb_doses": "nb_doses_flux_b_pfizer"}, axis=1)
  df_flux_moderna=df_flux_moderna.rename({"nb_doses": "nb_doses_flux_moderna"}, axis=1)

  df = df_flux_a_pfizer.merge(df_flux_b_pfizer, left_on=date_str, right_on=date_str, how="outer").merge(df_flux_moderna, left_on=date_str, right_on=date_str, how="outer").fillna(0)
  df["nb_doses_tot_cumsum"] = (df.nb_doses_flux_a_pfizer + df.nb_doses_flux_b_pfizer + df.nb_doses_flux_moderna).cumsum()

  return df

def import_data():
  df_flux_a_pfizer = pd.read_csv('data/input/flux-a-pfizer-nat.csv', sep=',')
  df_flux_b_pfizer = pd.read_csv('data/input/flux-b-pfizer-nat.csv', sep=',')
  df_flux_moderna = pd.read_csv('data/input/flux-moderna-nat.csv', sep=',')
  return prepare_data(df_flux_a_pfizer, df_flux_b_pfizer, df_flux_moderna)

def csv_to_json(df):
  dict_json = {"jour": list(df.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df.nb_doses_tot_cumsum)}

  with open("data/output/livraisons.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))


download_data()
df = import_data()
csv_to_json(df)
