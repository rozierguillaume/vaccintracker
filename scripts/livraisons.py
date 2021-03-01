import json
import pandas as pd
import requests
import numpy as np

vaccine_flows = {
  '6c39a8be-cd65-465a-a656-f8c645c839ac': 'flux-a-pfizer-nat',
  'f93be527-044b-4f33-baea-5e1c2f22e8aa': 'flux-b-pfizer-nat',
  'fb7ae18e-d49f-4008-9baf-f9af35152544': 'flux-moderna-nat',
  '6461ff61-3260-4576-b095-a9201dd64131': 'flux-astrazeneca-nat'
}

## FRANCE
def download_data():
  for vflow in vaccine_flows:
    url = "https://www.data.gouv.fr/fr/datasets/r/{0}".format(vflow)
    data = requests.get(url)

    with open('data/input/{0}.csv'.format(vaccine_flows[vflow]), 'wb') as f:
      f.write(data.content)

def download_tot_nat():
  url = "https://www.data.gouv.fr/fr/datasets/r/9c60af86-b974-4dba-bf34-f52686c7ada9"
  data = requests.get(url)

  with open('data/input/flux-tot-nat.csv', 'wb') as f:
          f.write(data.content)

def import_tot_nat():
  df = pd.read_csv('data/input/flux-tot-nat.csv', sep=',').groupby("date_debut_semaine").sum().reset_index()
  return df

def csv_to_json_tot_nat(df):
  dict_json = {"jour": list(df.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df.nb_doses.cumsum())}
  with open("data/output/flux-tot-nat.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))

def prepare_data(df_flux_a_pfizer, df_flux_b_pfizer, df_flux_moderna, df_flux_astrazeneca):
  date_str = "date_debut_semaine"

  df_flux_a_pfizer=df_flux_a_pfizer.rename({"nb_doses": "nb_doses_flux_a_pfizer"}, axis=1)
  df_flux_b_pfizer=df_flux_b_pfizer.rename({"nb_doses": "nb_doses_flux_b_pfizer"}, axis=1)
  df_flux_moderna=df_flux_moderna.rename({"nb_doses": "nb_doses_flux_moderna"}, axis=1)
  df_flux_astrazeneca=df_flux_astrazeneca.rename({"nb_doses": "nb_doses_flux_astrazeneca"}, axis=1)

  df = df_flux_a_pfizer.merge(df_flux_b_pfizer, left_on=date_str, right_on=date_str, how="outer")\
    .merge(df_flux_moderna, left_on=date_str, right_on=date_str, how="outer")\
    .merge(df_flux_astrazeneca, left_on=date_str, right_on=date_str, how="outer")\
    .fillna(0)
  df["nb_doses_tot_cumsum"] = (df.nb_doses_flux_a_pfizer + df.nb_doses_flux_b_pfizer + df.nb_doses_flux_moderna + df.nb_doses_flux_astrazeneca).cumsum()

  return df

def import_data():
  df_flux_a_pfizer = pd.read_csv('data/input/flux-a-pfizer-nat.csv', sep=',')
  df_flux_b_pfizer = pd.read_csv('data/input/flux-b-pfizer-nat.csv', sep=',')
  df_flux_moderna = pd.read_csv('data/input/flux-moderna-nat.csv', sep=',')
  df_flux_astrazeneca = pd.read_csv('data/input/flux-astrazeneca-nat.csv', sep=',')
  return prepare_data(df_flux_a_pfizer, df_flux_b_pfizer, df_flux_moderna, df_flux_astrazeneca)

def import_data_flux_separes():
  df_flux_a_pfizer = pd.read_csv('data/input/flux-a-pfizer-nat.csv', sep=',')
  df_flux_b_pfizer = pd.read_csv('data/input/flux-b-pfizer-nat.csv', sep=',')
  df_flux_pfizer = df_flux_a_pfizer.merge(df_flux_b_pfizer, left_on="date_debut_semaine", right_on="date_debut_semaine")
  df_flux_pfizer["nb_doses"] = df_flux_pfizer.nb_doses_x + df_flux_pfizer.nb_doses_y

  df_flux_moderna = pd.read_csv('data/input/flux-moderna-nat.csv', sep=',')
  df_flux_astrazeneca = pd.read_csv('data/input/flux-astrazeneca-nat.csv', sep=',')
  return df_flux_pfizer, df_flux_moderna, df_flux_astrazeneca


def csv_to_json(df):
  dict_json = {"jour": list(df.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df.nb_doses_tot_cumsum)}

  with open("data/output/livraisons.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))

def csv_to_json_flux_separes(df_flux_pfizer, df_flux_moderna, df_flux_astrazeneca):
  
  dict_json_pfizer = {"jour": list(df_flux_pfizer.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df_flux_pfizer.nb_doses.cumsum())}

  dict_json_moderna = {"jour": list(df_flux_moderna.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df_flux_moderna.nb_doses.cumsum())}

  dict_json_astrazeneca = {"jour": list(df_flux_astrazeneca.date_debut_semaine),
              "nb_doses_tot_cumsum": list(df_flux_astrazeneca.nb_doses.cumsum())}

  dict_json={1: dict_json_pfizer, 2: dict_json_moderna, 3: dict_json_astrazeneca}

  with open("data/output/livraisons-v.json", "w") as outfile:
    outfile.write(json.dumps(dict_json))

def main():
  download_data()
  df = import_data()
  csv_to_json(df)

  download_tot_nat()
  df = import_tot_nat()
  csv_to_json_tot_nat(df)

  df_flux_pfizer, df_flux_moderna, df_flux_astrazeneca = import_data_flux_separes()
  csv_to_json_flux_separes(df_flux_pfizer, df_flux_moderna, df_flux_astrazeneca)

if __name__ == "__main__":
  main()
