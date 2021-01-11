import json
import pandas as pd

with open('data_cumul_regions.json') as f:
  data_cumul_regions =  json.load(f)

for region in data_cumul_regions["liste_regions"]:
    df = pd.read_csv("data_regions/"+region+".csv")
    data_cumul_regions["donnees_regions"][region]["vaccines"] = df["nb_doses_injectees"].sum()

with open('data_cumul_regions.json', 'w') as f:
  json.dump(data_cumul_regions, f)
