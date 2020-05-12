# -*- coding: utf-8 -*-
"""
Created on Mon May 11 15:12:48 2020

@author: Lenovo
"""


import pymysql
import pandas as pd
import numpy as np
import math
import datetime
import time
from sqlalchemy import create_engine


#en_date = (datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")
#st_date = (datetime.datetime.now()-datetime.timedelta(days=7)).strftime("%Y-%m-%d")
#sql2 = 'select * from t_transportation_flow_forecast WHERE date between "' + st_date + '" and "' + en_date + '" '

def compare_js():
    engine = create_engine("mysql+pymysql://root:123456@52.1.123.6:3306/keenIts?charset=utf8")
    dat = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    con = pymysql.connect('52.1.123.6','root','123456','keenIts')
    sql1 = 'select * from t_transportation_flow_actual WHERE date = "' + dat + '" '
    sql2 = 'select * from t_transportation_flow_forecast WHERE date = "' + dat + '" '
    flow_actual = pd.read_sql(sql1,con)
    flow_forecast = pd.read_sql(sql2,con)
    con.close()
    
    p_nms = set(flow_actual['point_number'])&set(flow_forecast['point_number'])
    flow_actual1 = flow_actual[flow_actual['point_number'].isin(p_nms)].sort_values(by=['point_number','date','time_hour'])
    flow_forecast1 = flow_forecast[flow_forecast['point_number'].isin(p_nms)].sort_values(by=['point_number','date','time_hour'])
    
    cols = ['date','point_number','time_part', 'time_hour', 'time_index','flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time']
    flow_compare = pd.DataFrame(np.zeros((len(flow_forecast1),19)))
    flow_compare.columns = cols
    
    flow_compare['time_part'] = np.nan * len(flow_forecast1)
    flow_compare['time_index'] = np.nan * len(flow_forecast1)
    flow_compare['date'] = flow_actual1['date']
    flow_compare['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) * len(flow_forecast1)
    flow_compare['time_hour'] = flow_actual1['time_hour']
    flow_compare['point_number'] = flow_actual1['point_number']
    
    
    js_cols = ['flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r']
    flow_compare[js_cols] = (flow_forecast1[js_cols].values-flow_actual1[js_cols].values)/flow_actual1[js_cols].values
    
    #将结果写入数据库表t_transportation_forecast_test1
    flow_compare.to_sql('t_transportation_flow_compare',engine,schema='keenIts',if_exists='append',index=False,index_label=False)


compare_js()