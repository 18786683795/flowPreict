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
d1 = 1
def compare_js():
    engine = create_engine("mysql+pymysql://root:123456@52.1.123.6:3306/keenIts?charset=utf8")
    dat = (datetime.datetime.now()-datetime.timedelta(days=d1)).strftime("%Y-%m-%d")
    
    con = pymysql.connect('52.1.123.6','root','123456','keenIts')
    sql1 = 'select * from t_transportation_flow_actual WHERE date = "' + dat + '" '
    sql2 = 'select * from t_transportation_flow_forecast WHERE date = "' + dat + '" '
    flow_actual = pd.read_sql(sql1,con)
    flow_forecast = pd.read_sql(sql2,con)
    con.close()
    
    p_nms = set(flow_actual['point_number'])&set(flow_forecast['point_number'])
    flow_actual1 = flow_actual[flow_actual['point_number'].isin(p_nms)].sort_values(by=['point_number','date','time_hour'])
    flow_forecast1 = flow_forecast[flow_forecast['point_number'].isin(p_nms)].sort_values(by=['point_number','date','time_hour'])
    
    
    flow_compares1 = []
    for point in p_nms:
        flow_actual2 = flow_actual1[flow_actual1['point_number']==point]
        flow_forecast2 = flow_forecast1[flow_forecast1['point_number']==point]
        t_nms = set(flow_actual2['time_hour'])&set(flow_forecast2['time_hour'])
        flow_actual3 = flow_actual2[flow_actual2['time_hour'].isin(t_nms)].sort_values(by=['point_number','date','time_hour'])
        flow_forecast3 = flow_forecast2[flow_forecast2['time_hour'].isin(t_nms)].sort_values(by=['point_number','date','time_hour'])
        flow_forecast3 = flow_forecast3.drop_duplicates(['time_hour','flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r'])
        
        cols = ['date','point_number','time_part', 'time_hour', 'time_index','flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time']
        flow_compare = pd.DataFrame(np.zeros((len(flow_actual3),19)))
        flow_compare.columns = cols
        
        flow_compare['time_part'] = None 
        flow_compare['time_index'] = None
        flow_compare['date'] = flow_actual3['date'].values
        flow_compare['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        flow_compare['time_hour'] = flow_actual3['time_hour'].values
        flow_compare['point_number'] = flow_actual3['point_number'].values
        
        
        js_cols = ['flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r']
        flow_compare[js_cols] = (flow_forecast3[js_cols].values-flow_actual3[js_cols].values)/flow_actual3[js_cols].values
        for cl in js_cols:
            flow_compare[cl]=flow_compare[cl].map(lambda x:x if str(x)==None else round(x,7))
        flow_compares1.append(flow_compare)
        
    flow_compares = pd.concat(flow_compares1)
    flow_compares = flow_compares.replace(np.inf,np.nan)
    #将结果写入数据库表t_transportation_flow_compare
    flow_compares.to_sql('t_transportation_flow_compare',engine,schema='keenIts',if_exists='append',index=False,index_label=False)

compare_js()

