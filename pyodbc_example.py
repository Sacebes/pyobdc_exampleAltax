###############################################################
# First version May 17 2022
# AltaxExample: https://github.com/fbertos/atlax360-hi-exercise
# Contact: sandra.acebes@gmail.com
# 
################################################################

import pyodbc
import pandas as pd
import numpy as np


def sql_todf(connection:str) ->pd.DataFrame:
    
    #Item
    sql_query = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM Item
                               ''', connection)

    df_Item = pd.DataFrame(sql_query)
    
    #Customer
    sql_query2 = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM Customer
                               ''', connection)

    #df_Item = pd.DataFrame(sql_query, columns = ['product_id', 'product_name', 'price'])
    df_Customer = pd.DataFrame(sql_query2)
    return(df_Item,df_Customer)



def main():

    conn= pyodbc.connect(r'DRIVER=ODBC Driver 18 for SQL Server; SERVER=127.01;UID=sa; PWD=cmgYB2Zr4NJra2gRtGyjypag; TrustServerCertificate=YES')
    cur = conn.cursor()
    cur.execute('use ATLAX360_HI_DB')
    
    
    df_Item,df_Customer=sql_todf(conn)
    print(df_Item.info())
    

    #EJERCICIO
    
    #Set indexes
    #print(df_Customer[df_Customer.CustomerId.duplicated()]) #No hay clientes duplicados
    df_Customer.set_index('CustomerId')
    
    #Transform Item and set index

    df_Item.CreateDate=pd.to_datetime(df_Item['CreateDate'], format='YYYY%-MM%-DD:%H:%M:%S.%f')
    df_Item.UpdateDate=pd.to_datetime(df_Item['UpdateDate'], format='YYYY%-MM%-DD:%H:%M:%S.%f')

    
    df_Item.sort_values(by=['VersionNbr'], inplace=True)
    df_Item.drop_duplicates(subset="ItemId", keep="last", inplace= True)
    df_Item = df_Item[df_Item.DeletedFlag != 1]
    
    df_Item.set_index('ItemId')
    
    
    #Merge dataframes to map CustomerName
    df_final= df_Item.merge(df_Customer, on='CustomerId', how='outer').fillna('')
    
    #Define Local and drop NA
    df_final['Local'] = np.where((df_final['CustomerName'].str.endswith('66') == True) | (df_final['CustomerName'].str.endswith('99') == True), 'Local', 'External')
    
    df_final=df_final.dropna()
    
    print(df_final.info())
    
    #Select columns
    df_final=df_final[['ItemId','ItemDocumentNbr','CustomerName','CreateDate','UpdateDate','Local']]
    df_final.to_csv("file.csv.gz", compression='gzip', encoding="utf-8",sep=';',quotechar='"') 
    #df_final.to_csv("file2.csv", encoding="utf-8",sep=';',quotechar='"')
    
    conn.commit()
    cur.close()
    conn.close()



if __name__ == '__main__':
    main()