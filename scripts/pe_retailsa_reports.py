from  pe_bd_connection import ConnectionPostgre
import pandas as pd
import os

#Getting connection variable
objectConn = ConnectionPostgre()
conn_var = objectConn.get_conn()
schema_name_raw = 'raw_data'
schema_name_landing = 'landing'


#DF with BD data
def init_df_bd():
    df_deparment = pd.read_sql(f'select * from {schema_name_raw}.department', con=conn_var)
    df_orderItem = pd.read_sql(f'select * from {schema_name_raw}."orderItem"', con=conn_var)
    df_order = pd.read_sql(f'select * from {schema_name_raw}."order"', con=conn_var)
    df_category = pd.read_sql(f'select * from {schema_name_raw}.category', con=conn_var)
    df_customer = pd.read_sql(f'select * from {schema_name_raw}.customer', con=conn_var)
    df_product = pd.read_sql(f'select * from {schema_name_raw}.product', con=conn_var)
    return df_deparment,df_orderItem,df_order,df_category,df_customer,df_product

def get_departments_income(df_deparment,df_orderItem,df_order,df_category,df_product):
    df_dep_cat = df_deparment.merge(df_category, left_on='department_id', right_on='category_department_id', how='left')
    df_dep_cat_pro = df_dep_cat.merge(df_product, left_on='category_id', right_on='product_category_id', how='left')
    df_dep_cat_pro_ordI = df_dep_cat_pro.merge(df_orderItem, left_on='product_id', right_on='order_item_product_id', how='left')
    df_final_merge = df_dep_cat_pro_ordI.merge(df_order, left_on='order_item_order_id', right_on='order_id', how='left')
    df_final = df_final_merge[['department_name','order_item_subtotal','order_status']]
    df_final = df_final[df_final.order_item_subtotal.notnull()]
    df_result = df_final[df_final.order_status=='COMPLETE'].groupby(['department_name']).sum()
    df_result.reset_index(inplace=True)
    df_result.columns = ['department_name','total_income']
    df_result.to_sql('department_total_income', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)

def get_categories_purchases(df_orderItem,df_order,df_category,df_product):
    df_cat_pro = df_category.merge(df_product, left_on='category_id', right_on='product_category_id', how='left')
    df_cat_pro_ordI = df_cat_pro.merge(df_orderItem, left_on='product_id', right_on='order_item_product_id', how='left')
    df_final_merge = df_cat_pro_ordI.merge(df_order, left_on='order_item_order_id', right_on='order_id', how='left')
    df_final = df_final_merge[['category_name','order_item_quantity','order_status']]
    df_final = df_final[df_final.order_item_quantity.notnull()]
    df_result = df_final[df_final.order_status=='COMPLETE'].groupby(['category_name']).sum()
    df_result.reset_index(inplace=True) ##### OJOOOOOOO as_index=False como parametro del groupby
    df_result.columns = ['category_name','total_quantity']
    df_result['total_quantity'] = df_result['total_quantity'].astype("int64")
    df_result.to_sql('category_total_quantity', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)

def get_customers_purchases(df_orderItem,df_order,df_customer):
    df_cust_ord = df_customer.merge(df_order, left_on='customer_id', right_on='order_customer_id', how='left')
    df_final_merge = df_cust_ord.merge(df_orderItem, left_on='order_id', right_on='order_item_order_id', how='left')
    df_final = df_final_merge[['customer_fname','customer_lname','order_item_subtotal','order_status']]
    df_final = df_final[df_final.order_item_subtotal.notnull()]
    df_result = df_final[df_final.order_status=='COMPLETE'].groupby(['customer_fname','customer_lname']).sum()
    df_result.reset_index(inplace=True) ##### OJOOOOOOO as_index=False como parametro del groupby
    df_result.columns = ['customer_fname','customer_lname','total_purchase']
    df_result.to_sql('customer_total_purchase', con=conn_var, if_exists='replace',index = False,schema =schema_name_landing)


if __name__ == "__main__":
    df_deparment,df_orderItem,df_order,df_category,df_customer,df_product = init_df_bd()
    get_departments_income(df_deparment,df_orderItem,df_order,df_category,df_product)
    get_categories_purchases(df_orderItem,df_order,df_category,df_product)
    get_customers_purchases(df_orderItem,df_order,df_customer)