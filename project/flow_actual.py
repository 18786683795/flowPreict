import pymysql
import pandas as pd
import numpy as np
import time
import datetime
from datetime import timedelta
from sqlalchemy import create_engine


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
    point_list = ['#BY001','#BY002','#BY003','#BY004','#BY005','#BY006','#BY007','#BY008','#BY009','#BY010','#BY011','#BY012','#BY013','#BY014',
                  '#BY015','#GS001','#GS002','#GS004','#GS005','#GS006','#GS007','#GS008','#GS009','#GS010','#GS011','#GS012','#GS013','#GS015',
                  '#GS016','#GS017','#GS023','#GS024','#GS027','#GS028','#GS031','#GS032','#GS033','#GS034','#GS035','#GS037','#GS038','#GS039',
                  '#GS041','#GS043','#GS045','#GS047','#GS048','#GS049','#GS050','#GS051','#GS052','#GS057','#GS059','#GS060','#GS064','#GS065',
                  '#GS066','#GS067','#GS068','#GS069','#HX001','#HX002','#HX003','#HX004','#HX005','#HX006','#HX007','#HX008','#HX009','#HX010',
                  '#HX011','#HX012','#HX013','#HX014','#HX015','#HX016','#HX017','#HX018','#HX019','#HX020','#HX021','#HX022','#HX023','#HX024',
                  '#HX025','#HX026','#HX027','#HX028','#HX029','#HX030','#HX031','#HX032','#HX033','#HX034','#HX035','#HX036','#HX037','#WD001',
                  '#WD002','#WD003','#WD004','#WD005','#WD006','#WD007','#WD008','#WD009','#WD010','#WD011','#WD012','#WD013','#WD014','#WD015',
                  '#WD016','#WD017','#NM001','#NM002','#NM003','#NM004','#NM005','#NM006','#NM007','#NM008','#NM009','#NM010','#NM011','#NM012',
                  '#NM013','#NM014','#NM015','#NM016','#NM017','#NM018','#NM019','#NM020','#NM021','#NM022','#NM023','#NM024','#NM025','#NM026',
                  '#NM027','#NM028','#NM029','#NM030','#NM031','#NM032','#NM033','#NM034','#NM035','#NM036','#NM037','#NM038','#NM039','#NM040',
                  '#NM042','#NM043','#NM044','#NM045','#NM046','#NM047','#NM048','#NM049','#NM050','#NM051','#YY001','#YY002','#YY003','#YY004',
                  '#YY005','#YY006','#YY007','#YY008','#YY009','#YY010','#YY011','#YY012','#YY013','#YY014','#YY015','#YY016','#YY017','#YY018',
                  '#YY019','#YY020','#YY021','#YY022','#YY023','#YY024','#YY025','#YY026','#YY027','#YY028','#YY029','#YY030','#YY031','#YY032',
                  '#YY033','#YY034','#YY035','#YY036','#YY037','#YY038','#YY039','#YY040','#YY042','#YY043','#YY044','#YY046','#YY047','#YY048',
                  '#YY050','#YY051','#YY052','#YY053','#YY054','#YY055','#YY056','#YY057','#YY058','#YY059','#YY060','#YY061','#YY062','#YY063']
              
    #需要计算的日期
    yeday = (datetime.datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_list = list( pd.date_range(start=yeday,periods=1).astype(str)) 
    
    df = []
    for d in date_list:
        for r in point_list:
            sql = ('select * from t_transportation_flow where date = "'+d+'" and point_number = "'+r+'"')
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
            df_flow = df_flow.fillna(0)
         
            column_order = ['date','point_number', 'time_hour', 'flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time']
            df_flow.reindex(columns=list(column_order))
            cols = ['flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r']
            df_flow[cols] = df_flow[cols].astype('int')
            #导入数据库
            df_flow.to_sql('t_transportation_flow_actual',engine,schema='keenIts',if_exists='append',index=False,index_label=False) 

actual_js()

