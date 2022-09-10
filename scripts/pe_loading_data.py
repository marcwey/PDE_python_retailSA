from  pe_bd_connection import ConnectionPostgre
import pandas as pd
import os
from  datetime import datetime

schema_name = 'raw_data'

def cleaning_raw_data():

    # --------------- Categories Table ---------------------------
    df_Categories = pd.read_csv(f"{os.getcwd()}/data/categories",names=['category_id','category_department_id','category_name'],sep='|')
    
    # --------------- Customer Table ---------------------------
    df_PrevCustomer = pd.read_csv(f"{os.getcwd()}/data/customer",names=['customer_id','customer_fname','customer_lname','customer_email','customer_password','customer_street','customer_city','customer_state','customer_zipcode'],sep='|')
    df_PrevCustomer["customer_zipcode"] = df_PrevCustomer["customer_zipcode"].astype('str').str.zfill(5)
    df_Customer = df_PrevCustomer.loc[:, ['customer_id','customer_fname','customer_lname','customer_street','customer_city','customer_state','customer_zipcode']]

    # --------------- Departments Table ---------------------------
    df_Departments = pd.read_csv(f"{os.getcwd()}/data/departments",names=['department_id','department_name'],sep='|')

    # --------------- Order_items Table ---------------------------
    df_OrderItems = pd.read_csv(f"{os.getcwd()}/data/order_items",names=['order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price'],sep='|')
    
    # --------------- Orders Table ---------------------------
    df_PrevOrders = pd.read_csv(f"{os.getcwd()}/data/orders",names=['order_id','order_date','order_customer_id','order_status'],sep='|')
    df_PrevOrders['order_date'] = df_PrevOrders['order_date'].apply(lambda _: datetime.strptime(_,"%Y-%m-%d %H:%M:%S.%f"))
    df_Orders = df_PrevOrders

    # --------------- Products Table ---------------------------
    df_PrevProducts = pd.read_csv(f"{os.getcwd()}/data/products",names=['product_id','product_category_id','product_name','product_description','product_price','product_image'],sep='|')
    df_Products = df_PrevProducts.loc[:, ['product_id','product_category_id','product_name','product_price','product_image']]
    
    return df_Categories,df_Customer,df_Departments,df_OrderItems,df_Orders,df_Products

def insert_data_(df_Categories,df_Customer,df_Departments,df_OrderItems,df_Orders,df_Products):

    #getting connection object
    objectConn = ConnectionPostgre()
    conn_var = objectConn.get_conn()

    #loading DF data to postgre
    df_Categories.to_sql('category', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_Customer.to_sql('customer', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_Departments.to_sql('department', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_OrderItems.to_sql('orderItem', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_Orders.to_sql('order', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    df_Products.to_sql('product', con=conn_var, if_exists='replace',index = False,schema =schema_name)
    conn_var.close()

if __name__ == "__main__":
    df_Categories,df_Customer,df_Departments,df_OrderItems,df_Orders,df_Products = cleaning_raw_data()
    insert_data_(df_Categories,df_Customer,df_Departments,df_OrderItems,df_Orders,df_Products)