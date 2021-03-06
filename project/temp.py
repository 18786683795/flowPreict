import pymysql
import pandas as pd
import numpy as np
import time
import datetime
import math
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:123456@52.1.123.6:3306/keenIts?charset=utf8")

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

#需要预测的路口
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

#需要预测的日期
start_date = ((datetime.datetime.now()+datetime.timedelta(days=3)).strftime("%Y-%m-%d"))
date_list = list( pd.date_range(start_date,periods=1).astype(str)) 

for d in date_list:
    for p in point_list:
        sql = ('select date,point_number,time_part,time_hour,time_index,sum(flow_all) flow_all,sum(flow_e_l) flow_e_l,sum(flow_e_r) flow_e_r,\
        sum(flow_e_s) flow_e_s,sum(flow_w_l) flow_w_l,sum(flow_w_r) flow_w_r,sum(flow_w_s) flow_w_s,sum(flow_s_l) flow_s_l,sum(flow_s_r) flow_s_r,\
        sum(flow_s_s) flow_s_s,sum(flow_n_l) flow_n_l,sum(flow_n_r) flow_n_r,sum(flow_n_s) flow_n_s\
        from t_transportation_flow where date between "' 
        + (pd.to_datetime(d,format='%Y-%m-%d')+datetime.timedelta(days=-57)).strftime("%Y-%m-%d")
        + '" and "' + (pd.to_datetime(d,format='%Y-%m-%d')+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        + '" and WEEKDAY(date)=WEEKDAY("'+d+'") and point_number = "'+p
        +'" group by date,point_number,time_hour order by date,point_number,time_hour') 
        #取出历史八周数据
        result_all = get_mysql_data(sql)
                                                                                                                                                                                                                                                                                                              
        #历史八周对应日期
        date_list_merge = [(datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-56)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-49)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-42)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-35)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-28)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-21)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-14)).date(),
                           (datetime.datetime.strptime(d, "%Y-%m-%d")+datetime.timedelta(days=-7)).date()]
        date_merge = pd.DataFrame(date_list_merge, columns=['date'])
        
        #得到各方向转向columns的list
        columns_list = result_all.columns
        columns_list_keep = []
        for i in columns_list:
            if 'flow_' in i:
                columns_list_keep.append(i)
        columns_list_keep.remove('flow_all')


        hour_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]

        #权重计算
        zs = pd.Series([0.4*math.pow(0.6,7),0.4*math.pow(0.6,6),0.4*math.pow(0.6,5),0.4*math.pow(0.6,4),0.4*math.pow(0.6,3),
                        0.4*math.pow(0.6,2),0.4*0.6,0.4],index = [0,1,2,3,4,5,6,7])

        #创建一个空的dataframe
        forecast = pd.DataFrame(columns=['date','point_number','time_part', 'time_hour', 'time_index','flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time'])
        
        for h in hour_list:
            result = result_all[(result_all.point_number == p)&(result_all.time_hour == h)]
            #将缺数的日期补全
            result = pd.merge(date_merge, result, how='left', on='date')
            #index重置
            result = result.reset_index(drop=True)
            #按日期排序
            result = result.sort_values(by='date',ascending=True) 
            result['point_number'] = p
            result['time_hour'] = h
            
            #建立一个临时的预测dateframe
            df_forecast = result.loc[:,('date','point_number','time_part', 'time_hour', 'time_index','flow_all', 'flow_e_l', 'flow_e_s','flow_e_r', 'flow_w_l', 'flow_w_s', 'flow_w_r', 'flow_s_l', 'flow_s_s','flow_s_r', 'flow_n_l', 'flow_n_s', 'flow_n_r', 'create_time')]
            df_forecast['time_part'] = np.nan
            df_forecast['time_index'] = np.nan
            df_forecast = df_forecast.copy()
            df_forecast['date'] = d
            df_forecast = df_forecast.drop_duplicates(subset=['date','point_number','time_hour'],keep='first')
            
            for j in columns_list_keep:
                a = result[j]
                #计算流量缺失用于补数的数值
                if str(np.max(a)) == 'nan':
                    b =np.nan
                else:
                    b = int(np.max(a))*0.7
                
                #如果流量值为空或者小于b，那么把该值赋为b
                for r in range(len(result[j])):
                    if (result[j].isnull())[r]:
                        result[j][r] = b
                    elif result[j][r] < b:
                        result[j][r] = b
                #将数值类型转换为int
                result[j]=result[j].map(lambda x:x if str(x)=='nan' else int(x))
                #预测值计算
                df_forecast[j]=sum(result[j]*zs)+result[j][7]*math.pow(0.6,8)
                #create_time赋值为当前时间
                df_forecast['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                #将各方向求和作为总流量
                df_forecast['flow_all'] = df_forecast[[ 'flow_e_l','flow_e_s','flow_e_r','flow_w_l','flow_w_s','flow_w_r','flow_s_l','flow_s_s',
                                           'flow_s_r','flow_n_l','flow_n_s','flow_n_r']].sum(axis=1, skipna=True,min_count=1)
            forecast = forecast.append(df_forecast)
            for cl in columns_list_keep:
                forecast[cl]=forecast[cl].map(lambda x:x if str(x)=='nan' else int(x))
        #将结果写入数据库表t_transportation_forecast_test1
        forecast.to_sql('t_transportation_forecast_test1',engine,schema='keenIts',if_exists='append',index=False,index_label=False)

        
    
    

        
    
    

        
        
        
        