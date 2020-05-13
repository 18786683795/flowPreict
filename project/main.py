# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 15:15:41 2020

@author: Lenovo
"""

from datetime import datetime,timedelta
import time
import schedule
from flow_actual import *
from flow_forecast import *
from flow_compare import *

#每天8点定时启动
def main():
    schedule.every().day.at("02:00").do(actual_js)
    schedule.every().day.at("04:00").do(foreacst_js) 
    schedule.every().day.at("08:00").do(compare_js)
    while True:
        schedule.run_pending()
        time.sleep(20)
        
if __name__ == '__main__':
    main()