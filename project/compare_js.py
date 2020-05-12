# -*- coding: utf-8 -*-
"""
@author: Lenovo
"""

import pymysql
from sqlalchemy import create_engine
import datetime

def compare_js():
    conn = pymysql.connect(
            host='52.1.123.6',
            user='root',
            password='123456',
            db='keenIts',
            port=3306
        )
    cur = conn.cursor()    
    
    en_date = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
    st_date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    
    sql = 'INSERT into t_transportation_flow_compare(date,
    	point_number,
    	time_hour,
    	flow_all,
    	flow_e_l,
    	flow_e_r,
    	flow_e_s,
    	flow_w_l,
    	flow_w_r,
    	flow_w_s,
    	flow_s_l,
    	flow_s_r,
    	flow_s_s,
    	flow_n_l,
    	flow_n_r,
    	flow_n_s)
    SELECT
    	B.date,
    	B.point_number,
    	B.time_hour,
    	(B.flow_all-A.flow_all)/A.flow_all AS flow_all,
    	(B.flow_e_l-A.flow_e_l)/A.flow_e_l AS flow_e_l,
    	(B.flow_e_r-A.flow_e_r)/A.flow_e_r AS flow_e_r,
    	(B.flow_e_s-A.flow_e_s)/A.flow_e_s AS flow_e_s,
    	(B.flow_w_l-A.flow_w_l)/A.flow_w_l AS flow_w_l,
    	(B.flow_w_r-A.flow_w_r)/A.flow_w_r AS flow_w_r,
    	(B.flow_w_s-A.flow_w_s)/A.flow_w_s AS flow_w_s,
    	(B.flow_s_l-A.flow_s_l)/A.flow_s_l AS flow_s_l,
    	(B.flow_s_r-A.flow_s_r)/A.flow_s_r AS flow_s_r,
    	(B.flow_s_s-A.flow_s_s)/A.flow_s_s AS flow_s_s,
    	(B.flow_n_l-A.flow_n_l)/A.flow_n_l AS flow_n_l,
    	(B.flow_n_r-A.flow_n_r)/A.flow_n_r AS flow_n_r,
    	(B.flow_n_s-A.flow_n_s)/A.flow_n_s AS flow_n_s
    FROM
    	(
    		SELECT
    			date,
    			point_number,
    			time_hour,
    			flow_all,
    			flow_e_l,
    			flow_e_r,
    			flow_e_s,
    			flow_w_l,
    			flow_w_r,
    			flow_w_s,
    			flow_s_l,
    			flow_s_r,
    			flow_s_s,
    			flow_n_l,
    			flow_n_r,
    			flow_n_s
    		FROM
    			t_transportation_flow_hour
    		WHERE
    			date between 
            "' 
            + st_date
            + '" and "' + en_date
            + '"
    		GROUP BY
    			date,
    			point_number,
    			time_hour
    	) A,
    	t_transportation_forecast_test1 B
    WHERE A.date=B.date and A.point_number=B.point_number and A.time_hour=B.time_hour;'
    
    cur.execute(sql)  
    conn.close()
    

compare_js()







#ceshi
#import pymysql
#from sqlalchemy import create_engine
#import datetime
#import pandas as pd
#
#
#con = pymysql.connect('52.1.123.6','root','123456','keenIts')
#
#st_date = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
#en_date = (datetime.datetime.now()+datetime.timedelta(days=7)).strftime("%Y-%m-%d")
#
#sql = ('select * from t_transportation_flow_compare where date between "' 
#        + st_date
#        + '" and "' + en_date
#        + '"') 
#
#data = pd.read_sql(sql,con)  
#print(data)