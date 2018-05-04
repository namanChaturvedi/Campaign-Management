# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 13:30:42 2017

@author: vn0bz25
"""
import datetime
import dateutil.relativedelta
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import *
from pyspark.sql import Row
import EnumFile as ef

def converting_to_dict(name, cols):
    nwLst = [x for x in cols if x != '']
    Flag = name
    Operation = nwLst[0]
    FilterClause = nwLst[1]
    TimePeriod = nwLst[2]
    EndDate = nwLst[3]
    Values = nwLst[4:]
    return(Flag, Operation, FilterClause, TimePeriod, EndDate, Values)
    
def functoBin(qty, bucktLst):
    if(qty == 0):
        return 0
    elif(qty >= int(bucktLst[len(bucktLst)-1].split('+')[0])):
        return bucktLst[len(bucktLst)-1]
    else:
        for item in bucktLst[1:len(bucktLst)-1]:
            if int(item.split('to')[0]) <= qty <= int(item.split('to')[1]):
                return item
                break


#udffunctoBin = udf(lambda c: functoBin(c, bucktLst), StringType())

def funcUpcForBins(rdd, dicInput, bucktLst, udffunctoBin):
    return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
    .withColumn('upc_present',when(col('upc_nbr').isin(map(int,dicInput[ef.Inputs.Values])),col('upc_nbr')).otherwise(0)) \
    .select('household_id','upc_present') \
    .groupBy('household_id') \
    .withColumn('sum_qty', when(col('upc_present')> 0, f.sum(f.col('unit_qty'))).otherwise(0)) \
    .withColumn(dicInput[ef.Inputs.FlagName],udffunctoBin(col('sum_qty'))) \
    .select(col('household_id').alias('hh_id'),col(dicInput[ef.Inputs.FlagName]))  
    
    
def funcSubCatForBins(rdd, dicInput, bucktLst, udffunctoBin):
    return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
    .withColumn('subcatg_present',when(col('dept_subcatg_nbr').isin(map(int,dicInput[ef.Inputs.Values])),col('dept_subcatg_nbr')).otherwise(0)) \
    .select('household_id','subcatg_present') \
    .dropDuplicates() \
    .withColumn('subcatg_count', when(col('subcatg_present')> 0, 1).otherwise(0)) \
    .groupBy('household_id').sum('subcatg_count') \
    .withColumn(dicInput[ef.Inputs.FlagName],udffunctoBin(col('sum(subcatg_count)'))) \
    .select(col('household_id').alias('hh_id'),col(dicInput[ef.Inputs.FlagName]))
    
       
def periodCalc(enddate, period):
    return last_day_of_month((datetime.datetime.strptime(enddate,"%Y-%m-%d") \
    - dateutil.relativedelta.relativedelta(months = int(period))).date()) \
    + dateutil.relativedelta.relativedelta(days = 1)
    
def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)
