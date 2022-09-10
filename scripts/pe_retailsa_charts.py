from  pe_bd_connection import ConnectionPostgre
import pandas as pd
##import os
## from  datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

objectConn = ConnectionPostgre()
conn_var = objectConn.get_conn()
schema_name_landing = 'landing'


#DF with BD data
df_deparment_total_income = pd.read_sql(f'select * from {schema_name_landing}.department_total_income', con=conn_var)

df_categories_total_quantity = pd.read_sql(f'select * from {schema_name_landing}.category_total_quantity', con=conn_var)

df_customer_total_purchase = pd.read_sql(f'select * from {schema_name_landing}.customer_total_purchase', con=conn_var)

##print (df_deparment_total_income.head())

def bar_chart_department():
    plt.figure(figsize=(20,20))
    x_keys = df_deparment_total_income["department_name"]
    y_values = df_deparment_total_income["total_income"]
    plt.bar(x_keys, y_values)
    plt.xticks(x_keys, rotation='horizontal', size=10)
    plt.show()
    
def bar_chart_categories():
    df_categories_final = df_categories_total_quantity.sort_values(by= "total_quantity", ascending= False).head(10)
    plt.figure(figsize=(20,20),  dpi= 80) ### 50 dpi
    x_keys = df_categories_final["category_name"]
    y_values = df_categories_final["total_quantity"]
    plt.bar(x_keys, y_values)
    plt.xticks(x_keys, rotation='horizontal', size=8)
    plt.show()
    
def bar_chart_customer():
    df_customer_final = df_customer_total_purchase.sort_values(by= "total_purchase", ascending= False).head(5)
    plt.figure(figsize=(15,30))
    x_keys = df_customer_final["customer_fname"]+ " " + df_customer_final["customer_lname"]
    y_values = df_customer_final["total_purchase"]
    plt.bar(x_keys, y_values)
    plt.xticks(x_keys, rotation='horizontal', size=10)
    plt.show()
    

    
if __name__ == "__main__":
    bar_chart_department()
    bar_chart_categories()
    bar_chart_customer()