# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 15:36:00 2017
Description - This is supporting script for Scoring module require to identify filtering operation to be performed, and 
        generating date for respective period.
@author: vn0bz25
"""
import datetime
import dateutil.relativedelta

keysMapping = {('department',): "dept",
               ('department','category'): "catg",
                ('category','department'): "catg",
                ('department','subcategory'): "subcatg",
                ('category','subcategory','fineline'):"fineline",
                ('subcategory','category','fineline'):"fineline",
                ('fineline'):"fineline",
                ('subcategory','department'): "subcatg",
                ('subcategory',): "subcatg",
                ('category',): "catg",
                ('department', 'upc'): "upc",
                ('upc',): "upc"}
                
def periodCalc(enddate, period):
    return last_day_of_month((datetime.datetime.strptime(enddate,"%Y-%m-%d") 
    - dateutil.relativedelta.relativedelta(months = int(period))).date()) \
    + dateutil.relativedelta.relativedelta(days = 1)
    
def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)
    
