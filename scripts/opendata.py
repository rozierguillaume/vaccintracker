import json
import pandas as pd
import requests

## FRANCE
def download_fra_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/efe23314-67c4-45d3-89a2-3faef82fae90"
  data = requests.get(url)

  with open('data/input/vacsi-fra.csv', 'wb') as f:
          f.write(data.content)

def import_fra_data():
  df = pd.read_csv('data/input/vacsi-fra.csv', sep=';')
  return df

def csv_to_json_fra(df):

  dict_json = {}
  dict_json["dates"] = df["jour"].tolist()
  dict_json["n_dose1"] = df["n_dose1"].tolist()
  dict_json["n_cum_dose1"] = df["n_cum_dose1"].tolist()
  dict_json["n_dose1_moyenne7j"] = round(df["n_dose1"].rolling(window=7).mean()).fillna(0).tolist()

  dict_json["n_dose3"] = df["n_rappel"].tolist()
  dict_json["n_cum_dose3"] = df["n_cum_rappel"].tolist()
  dict_json["n_dose3_moyenne7j"] = round(df["n_rappel"].rolling(window=7).mean()).fillna(0).tolist()

  dict_json["n_complet"] = df["n_complet"].tolist()
  dict_json["n_cum_complet"] = df["n_cum_complet"].tolist()
  dict_json["n_complet_moyenne7j"] = round(df["n_complet"].rolling(window=7).mean()).fillna(0).tolist()

  with open("data/output/vacsi-fra.json", "w") as outfile: 
    outfile.write(json.dumps(dict_json))


## REGIONS
def ajouterZeroNumeroRegion(num):
  if len(num) == 1:
    return "0" + str(num)
  else:
    return num
def prepare_reg_data(df):
  df_noms = pd.read_csv('data/input/reg-noms.csv', sep=',')
  return df.merge(df_noms, left_on="reg", right_on="reg")

def download_reg_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/735b0df8-51b4-4dd2-8a2d-8e46d77d60d8"
  data = requests.get(url)

  with open('data/input/vacsi-reg.csv', 'wb') as f:
          f.write(data.content)

def import_reg_data():
  df = pd.read_csv('data/input/vacsi-reg.csv', sep=';')
  return prepare_reg_data(df)

def csv_to_json_reg(df):
  pop_reg = pd.read_csv("data/input/reg-pop.csv", sep=";")

  list_json = []
  regions = df.reg[df.reg != 7].unique().tolist()

  for reg in regions:
    df_reg = df[df.reg == reg].sort_values(by="jour").reset_index()
    
    df_reg["n_dose1_cumsum"] = df_reg["n_dose1"].cumsum()
    df_reg["n_dose1_cumsum_moyenne7j"] = df_reg["n_dose1"].rolling(window=7).mean().fillna(0)

    pop = pop_reg[pop_reg.reg == reg]["population"].tolist()[0]

    for i in range(len(df_reg)):
      dict_json = {}
      dict_json["date"] = df_reg.loc[i,"jour"]
      dict_json["code"] = "REG-" + ajouterZeroNumeroRegion(str(df_reg.loc[i,"reg"]))
      dict_json["nom"] = df_reg.loc[i, "libelle"]
      dict_json["n_dose1_cumsum"] = int(df_reg.loc[i, "n_dose1_cumsum"])
      dict_json["n_dose1_cumsum_moyenne7j"] = df_reg.loc[i, "n_dose1_cumsum_moyenne7j"]
      dict_json["n_dose1_pourcent_pop"] = round(dict_json["n_dose1_cumsum"]/pop*100, 1)

      list_json += [dict_json]

  with open("data/output/vacsi-reg.json", "w") as outfile: 
    outfile.write(json.dumps(list_json))


## DEPARTEMENTS
def download_dep_data():
  url = "https://www.data.gouv.fr/fr/datasets/r/83cbbdb9-23cb-455e-8231-69fc25d58111"
  data = requests.get(url)

  with open('data/input/vacsi-a-dep.csv', 'wb') as f:
          f.write(data.content)

def checkNumeroDep(dep):
  if(len(dep)==1):
    return "0"+dep
  else:
    return dep

def import_dep_data():
  df = pd.read_csv('data/input/vacsi-a-dep.csv', sep=';')
  df.dep = df.dep.astype(str)
  df.dep = df.dep.apply(lambda x: checkNumeroDep(x))
  return df

def csv_to_json_dep(df):
  df = df[df["clage_vacsi"]==0]
  deps = df["dep"].unique().tolist()
  pop_dep = pd.read_csv('data/input/dep-pop.csv', sep=';')
  df = df.merge(pop_dep, left_on="dep", right_on="dep")

  dict_json = {"departements": deps}

  for dep in deps:
    df_dep = df[df.dep == dep].sort_values(by="jour")
    if(len(df_dep)):
      df_dep["n_dose1_cumsum"] = df_dep["n_dose1"].cumsum()
      dict_json[dep] = {"dates": df_dep.jour.tolist(), 
                        "n_dose1_cumsum": df_dep.n_dose1_cumsum.tolist(), 
                        "n_dose1_cumsum_pop": round((df_dep.n_dose1_cumsum.values[-1] / df_dep["departmentPopulation"].values[-1]) * 100, 2)
                        }

  with open("data/output/vacsi-dep.json", "w") as outfile: 
    outfile.write(json.dumps(dict_json, indent=4))


download_fra_data()
df = import_fra_data()
csv_to_json_fra(df)

download_reg_data()
df = import_reg_data()
csv_to_json_reg(df)

download_dep_data()
df = import_dep_data()
csv_to_json_dep(df)
