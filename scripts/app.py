from  pe_bd_connection import ConnectionPostgre
import pandas as pd
import json
from flask import Flask,jsonify,request


#Getting connection variable
objectConn = ConnectionPostgre()
conn_var = objectConn.get_conn()
schema_name_landing = 'landing'


#REST API
app = Flask(__name__)

@app.route('/retailSA/reports/departments',methods=['POST'])

#Options in department_name : all,specific_name
#Options for Top X department : TOP number,''(empty)
def get_report_departments():
    df_deparment_report = pd.read_sql(f'select * from {schema_name_landing}.department_total_income', con=conn_var)

    if request.json['department_name'] == 'all':

        if request.json['top_number'] == '':

            result = df_deparment_report.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':'Incomes by departments'})

        else:

            top_number = int(request.json['top_number'])
            df_deparment_report_final = df_deparment_report.sort_values(by='total_income',ascending=False).head(top_number)
            result = df_deparment_report_final.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':f'Top {top_number} departments'})

    else:

        department_name_var = request.json['department_name']
        df_result = df_deparment_report[df_deparment_report['department_name']==department_name_var]
        result = df_result.to_json(orient="records")
        json_result = json.loads(result)
        return jsonify({'result':json_result,'message':f'Income for {department_name_var} department'})

@app.route('/retailSA/reports/categories',methods=['POST'])

#Options in category_name : all,specific_name
#Options for Top X category : TOP number,''(empty)
def get_report_categories():
    df_category_report = pd.read_sql(f'select * from {schema_name_landing}.category_total_quantity', con=conn_var)

    if request.json['category_name'] == 'all':

        if request.json['top_number'] == '':

            result = df_category_report.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':'Total quantity by categories'})

        else:

            top_number = int(request.json['top_number'])
            df_category_report_final = df_category_report.sort_values(by='total_quantity',ascending=False).head(top_number)
            result = df_category_report_final.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':f'Top {top_number} categories'})

    else:

        category_name_var = request.json['category_name']
        df_result = df_category_report[df_category_report['category_name']==category_name_var]
        result = df_result.to_json(orient="records")
        json_result = json.loads(result)
        return jsonify({'result':json_result,'message':f'Total quantity for {category_name_var} category'})

@app.route('/retailSA/reports/customers',methods=['POST'])

#Options in customer_fullNaeme : all,specific_fullName
#Options for Top X customer : TOP number,''(empty)
def get_report_customers():
    df_customer_report = pd.read_sql(f'select * from {schema_name_landing}.customer_total_purchase', con=conn_var)

    if request.json['customer_fullName'] == 'all':

        if request.json['top_number'] == '':

            result = df_customer_report.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':'Total purchase by customers'})

        else:

            top_number = int(request.json['top_number'])
            df_customer_report_final = df_customer_report.sort_values(by='total_purchase',ascending=False).head(top_number)
            result = df_customer_report_final.to_json(orient="records")
            json_result = json.loads(result)
            return jsonify({'result':json_result,'message':f'Top {top_number} customers'})

    else:

        customer_fullName_var = request.json['customer_fullName']
        customer_fname = customer_fullName_var.split(' ')[0]
        customer_lname = customer_fullName_var.split(' ')[1]
        df_result_prev = df_customer_report[df_customer_report['customer_fname']==customer_fname]
        df_result = df_result_prev[df_result_prev['customer_lname']==customer_lname]
        result = df_result.to_json(orient="records")
        json_result = json.loads(result)
        return jsonify({'result':json_result,'message':f'Total purchase for {customer_fullName_var} customer'})


if __name__ == "__main__":
    app.run(debug=True,port=4000)

