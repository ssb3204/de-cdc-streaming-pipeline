import pandas as pd
import os

base = r"C:\Users\ryanp\OneDrive\바탕 화면\de-project\de-cdc-streaming-pipeline\data"
df_all = pd.read_csv(os.path.join(base, "olist_orders_dataset.csv"))
df_70  = pd.read_csv(os.path.join(base, "orders_initial_70.csv"))

print(len(df_all), len(df_70), len(df_70) / len(df_all))
