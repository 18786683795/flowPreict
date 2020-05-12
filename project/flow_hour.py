# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 16:51:34 2019

@author: Lenovo
"""

import pymysql
import pandas as pd
import numpy as np
import time
import datetime
from datetime import timedelta
from sqlalchemy import create_engine

def date_processing():
    def rq_zh(d):
        today = datetime.datetime.now() #取今天
        yeday = (today + timedelta(days=d)).strftime("%Y-%m-%d")
        return yeday
    dats = []
    for i in range(0,8):
        dat = rq_zh(i)
        dats.append(dat)
    return dats

def actual_js():
    def get_mysql_data(sql):
        """
        提取mysql中的数据并返回成dataframe
        参数只需要sql语句
    
        """
        conn = pymysql.connect(
            host='52.1.123.6',
            user='root',
            password='123456',
            db='keenIts',
            port=3306
        )
        cur = conn.cursor()  # 获取操作游标，也就是开始操作
        sql_select = sql  # 查询命令
        cur.execute(sql_select)  # 执行查询语句
    
        result = cur.fetchall()  # 获取查询结果
        col_result = cur.description  # 获取查询结果的字段描述
    
        columns = []
        for i in range(len(col_result)):
            columns.append(col_result[i][0])  # 获取字段名，以列表形式保存
    
        df = pd.DataFrame(columns=columns)
        for i in range(len(result)):
            df.loc[i] = list(result[i])  # 按行插入查询到的数据
    
        conn.close()  # 关闭数据库连接
    
        return df
    
    #需要计算的路口
    #con = pymysql.connect('52.1.123.6','root','123456','keenIts')
    #sql = "select number from t_transportation;"
    #point_list = pd.read_sql(sql,con)['number']

    point_list = ['#GS001','#GS002','#GS004','#GS005','#GS006','#GS007','#GS008','#GS009','#GS010','#GS011','#GS012','#GS013','#GS015','#GS016',
                  '#GS017','#GS023','#GS024','#GS027','#GS028','#GS031','#GS032','#GS033','#GS034','#GS035','#GS037','#GS038','#GS039','#GS041',
                  '#GS043','#GS045','#GS047','#GS048','#GS049','#GS050','#GS051','#GS052','#GS057','#GS059','#GS060','#GS064','#GS065','#GS066',
                  '#GS067','#GS068','#GS069']
    
    #需要计算的日期（以下代码为上一周）
    date_list = [(datetime.datetime.now()+datetime.timedelta(days=-7)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-6)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-5)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-4)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-3)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-2)).strftime("%Y-%m-%d"),
                (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")]
    
    for d in date_list:
        for r in point_list:
            sql = ('select * from t_transportation_flow_actual where date = "'+d+'" and point_number = "'+r+'"')
            result_all = get_mysql_data(sql)
        
            sql2 = ('select * from t_transportation_flow_forecast where date = "'+d+'" and point_number = "'+r+'" \
                order by date,time_hour,point_number')
            forecast_sum_all = get_mysql_data(sql2)
    
            #result_all['date_hour']=result_all['date'].map(str)+'-'+result_all['time_hour'].map(str)
    
            columns_list = result_all.columns
            columns_list_keep = []
            for j in columns_list:
                if 'flow_' in j:
                    columns_list_keep.append(j)
            columns_list_keep.remove('flow_all')
            columns_list_keep
    
            engine = create_engine("mysql+pymysql://root:123456@52.1.123.6:3306/keenIts?charset=utf8")
    
            result = result_all[result_all.point_number == r]
            forecast_sum = forecast_sum_all[forecast_sum_all.point_number == r]
            forecast_sum = forecast_sum.reset_index(drop=True)
            df_flow = result[['point_number','time_hour']]
            df_flow = df_flow.drop_duplicates(subset=['time_hour','point_number'],keep='first')
            df_flow = df_flow.sort_values(by='time_hour',ascending=True)
            
            for i in columns_list_keep:
               #按小时分组求流量和
                df1 = result.groupby('time_hour')
                a = df1[i].sum()
                c = round(a)
                dict_c = {'time_hour':c.index,i:c.values}
                df_c = pd.DataFrame(dict_c)
                df_flow = pd.merge(left = df_flow, right = df_c,how = 'left',left_on='time_hour',right_on = 'time_hour')
            df_flow['date'] = d
            df_flow['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            df_flow['flow_all'] = df_flow[[ 'flow_e_l','flow_e_s','flow_e_r','flow_w_l','flow_w_s','flow_w_r','flow_s_l','flow_s_s',
             'flow_s_r','flow_n_l','flow_n_s','flow_n_r']].sum(axis=1, skipna=True,min_count=1)
            forecast_sum = forecast_sum[['time_hour','flow_all']]
            df_flow = pd.merge(left = df_flow, right = forecast_sum,how = 'left',left_on='time_hour',right_on = 'time_hour')
    
            #如果实测流量小于预测流量的0.8，那么把实测总流量置空
            for l in range(len(df_flow['time_hour'])):
                if df_flow['flow_all_x'][l] < int(df_flow['flow_all_y'][l])*0.8:
                    df_flow['flow_all_x'][l]=np.nan
            
            del df_flow['flow_all_y']
            df_flow = df_flow.rename(columns={'flow_all_x':'flow_all'})
            
            column_order = ['date','point_number', 'time_hour', 'flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time']
            df_flow.reindex(columns=list(column_order))
            #导入数据库
            print(df_flow)
            
            con = pymysql.connect('52.1.123.6','root','123456','keenIts')
            sql = "select DISTINCT(date) from t_transportation_flow_actual ORDER BY date desc limit 7;"
            databas_datelist = pd.read_sql(sql,con)['date']
            pred_datelist = date_processing()
            xt_rq = [x for x in databas_datelist if x in pred_datelist]
            bt_rq = [x for x in (str(databas_datelist)+str(pred_datelist)) if x not in xt_rq]
            df_flow0 = df_flow[df_flow['date'].isin(bt_rq)]
            
    #        df_flow0.to_sql('t_transportation_flow_actual',engine,schema='keenIts',if_exists='append',index=False,index_label=False) 


actual_js()


