# -*- coding: utf-8 -*-
"""
Created on Tue Mar 07 11:10:13 2017

@author: vn0bz25
"""
from __future__ import division
import datetime
import dateutil.relativedelta
import EnumFile as ef
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, StructField, IntegerType

##Function to convert input csv to python data structure
def converting_to_dict(name, cols):
    nwLst = [x for x in cols if x != '']
    Flag = name
    Operation = nwLst[0]
    FilterClause = nwLst[1]
    TimePeriod = nwLst[2]
    EndDate = nwLst[3]
    Values = nwLst[4:]
    return(Flag, Operation, FilterClause, TimePeriod, EndDate, Values)
    
##Function to Generate Purchase flag when UPCs are passed
def funcSumOperation(rdd, dicInput):
    if dicInput[ef.Inputs.FilterClause] != 'loyalty':
        return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('upc_present',f.when(f.col('upc_nbr').isin(map(int,dicInput[ef.Inputs.Values])),1).otherwise(0)) \
        .select('household_id','retail_price','upc_present') \
        .groupBy('household_id').sum('retail_price','upc_present') \
        .select(f.col('household_id'), f.col('sum(retail_price)'), f.col('sum(upc_present)')) \
        .withColumn(dicInput[ef.Inputs.FlagName],f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(upc_present)') >= 1), 1).otherwise(None)) \
        .select(f.col('household_id').alias('hh_id'),f.col(dicInput[ef.Inputs.FlagName]))
    else:
        intermediateRdd = rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('upc_present',f.when(f.col('upc_nbr').isin(map(int,dicInput[ef.Inputs.Values])),1).otherwise(0)) \
        .select('household_id','retail_price','upc_present') 
        
        return intermediateRdd.filter(f.col('upc_present') == 1) \
        .groupBy('household_id').sum('retail_price','upc_present') \
        .select(f.col('household_id'), f.col('sum(retail_price)'), f.col('sum(upc_present)')) \
        .withColumn(dicInput[ef.Inputs.FlagName],f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(upc_present)') >= 1), 1).otherwise(None)) \
        .withColumn(str(dicInput[ef.Inputs.FlagName])+'_sale', f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(upc_present)') >= 1), f.col('sum(retail_price)')).otherwise(0)) \
        .select(f.col('household_id').alias('hh_id'),f.col(dicInput[ef.Inputs.FlagName]), f.col(str(dicInput[ef.Inputs.FlagName])+'_sale'))
    
def funcSubCatOperation(rdd, dicInput):
    if dicInput[ef.Inputs.FilterClause] != 'loyalty':
        return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('subcat_present',f.when(f.col('dept_subcatg_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'subcat_present').groupBy(f.col('household_id')) \
        .sum('subcat_present') \
        .withColumn(dicInput[ef.Inputs.FlagName], f.when(f.col('sum(subcat_present)') > 0, 1).otherwise(None)) \
        .select(f.col('household_id').alias('hh_id'), f.col(dicInput[ef.Inputs.FlagName]))
    else:
        intermediateRdd = rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('subcat_present',f.when(f.col('dept_subcatg_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'retail_price', 'subcat_present')
        
        return intermediateRdd.filter(f.col('subcat_present') == 1) \
        .groupBy(f.col('household_id')) \
        .sum('retail_price','subcat_present') \
        .withColumn(dicInput[ef.Inputs.FlagName],f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(subcat_present)') >= 1), 1).otherwise(None)) \
        .withColumn(str(dicInput[ef.Inputs.FlagName])+'_sale', f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(subcat_present)') >= 1), f.col('sum(retail_price)')).otherwise(0)) \
        .select(f.col('household_id').alias('hh_id'),f.col(dicInput[ef.Inputs.FlagName]), f.col(str(dicInput[ef.Inputs.FlagName])+'_sale'))
        
def funcSubcatTrips(rdd, dicInput):
    return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
    .where(rdd.dept_subcatg_nbr.isin(map(int,dicInput[ef.Inputs.Values])))\
    .select('household_id','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key')))\
    .groupBy('household_id').agg(f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(dicInput[ef.Inputs.FlagName])))\
    .select(f.col('household_id').alias('hh_id'), f.col(dicInput[ef.Inputs.FlagName]))
    
def funcUpcQtyOperation(rdd, dicInput):
    return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .where(rdd.upc_nbr.isin(map(int,dicInput[ef.Inputs.Values])))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .select(f.col('household_id').alias('hh_id'), f.col("sum(unit_qty)").alias(dicInput[ef.Inputs.FlagName]))
   
def funcCatOperation(rdd, dicInput):
    if dicInput[ef.Inputs.FilterClause] != 'loyalty':
        return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('cat_present',f.when(f.col('dept_category_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'cat_present').groupBy(f.col('household_id')) \
        .sum('cat_present') \
        .withColumn(dicInput[ef.Inputs.FlagName], f.when(f.col('sum(cat_present)') > 0, 1).otherwise(None)) \
        .select(f.col('household_id').alias('hh_id'), f.col(dicInput[ef.Inputs.FlagName]))
    else:
        intermediateRdd = rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('cat_present',f.when(f.col('dept_category_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'retail_price','cat_present')
        
        return intermediateRdd.filter(f.col('cat_present') == 1) \
        .groupBy(f.col('household_id')) \
        .sum('retail_price','cat_present') \
        .withColumn(dicInput[ef.Inputs.FlagName],f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(cat_present)') >= 1), 1).otherwise(None)) \
        .withColumn(str(dicInput[ef.Inputs.FlagName])+'_sale', f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(cat_present)') >= 1), f.col('sum(retail_price)')).otherwise(0)) \
        .select(f.col('household_id').alias('hh_id'),f.col(dicInput[ef.Inputs.FlagName]), f.col(str(dicInput[ef.Inputs.FlagName])+'_sale'))
        
def funcDeptOperation(rdd, dicInput):
    if dicInput[ef.Inputs.FilterClause] != 'loyalty':
        return rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('dept_present',f.when(f.col('acctg_dept_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'dept_present').groupBy(f.col('household_id')) \
        .sum('dept_present') \
        .withColumn(dicInput[ef.Inputs.FlagName], f.when(f.col('sum(dept_present)') > 0, 1).otherwise(None)) \
        .select(f.col('household_id').alias('hh_id'), f.col(dicInput[ef.Inputs.FlagName]))
    else:
        intermediateRdd = rdd.filter(rdd.visit_date >= periodCalc(dicInput[ef.Inputs.EndDate], dicInput[ef.Inputs.TimePeriod])) \
        .withColumn('dept_present',f.when(f.col('acctg_dept_nbr').isin(map(int,dicInput[ef.Inputs.Values])), 1).otherwise(None)) \
        .select('household_id', 'retail_price', 'dept_present')
        
        return intermediateRdd.filter(f.col('dept_present') == 1) \
        .groupBy(f.col('household_id')) \
        .sum('retail_price','dept_present') \
        .withColumn(dicInput[ef.Inputs.FlagName],f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(dept_present)') >= 1), 1).otherwise(None)) \
        .withColumn(str(dicInput[ef.Inputs.FlagName])+'_sale', f.when((f.col('sum(retail_price)') > 0) & (f.col('sum(dept_present)') >= 1), f.col('sum(retail_price)')).otherwise(0)) \
        .select(f.col('household_id').alias('hh_id'),f.col(dicInput[ef.Inputs.FlagName]), f.col(str(dicInput[ef.Inputs.FlagName])+'_sale'))

def funcComboOperation(rdd, dicInput, hc):
#    comboRdd = rdd.withColumn(dicInput[ef.Inputs.FlagName], \
#    when((col(dicInput[ef.Inputs.Values][0]) == 1) & (col(dicInput[ef.Inputs.Values][1]) == 1), 2) \
#    .when((col(dicInput[ef.Inputs.Values][0]) == 1) | (col(dicInput[ef.Inputs.Values][1]) == 1), 1) \
#    .otherwise(0))
    comboRdd = rdd.withColumn(dicInput[ef.Inputs.FlagName], sum(f.col(x) for x in dicInput[ef.Inputs.Values])) 
    comboRdd.registerTempTable('CrossTab')
    del comboRdd
    
def funcHoldoutOperation(rdd, dicInput):
    return rdd.sample(False, 0.2, seed = 123).withColumn(dicInput[ef.Inputs.FlagName], f.lit(1)) \
    .select(f.col('household_id').alias('hh_id'), f.col(dicInput[ef.Inputs.FlagName]))
        
def funcLoyaltyOperation(rdd, dicInput, hc):
    colLst = list(rdd.columns)
    valLst = [x+'_sale' for x in dicInput[ef.Inputs.Values]]
    colLst = [x for x in colLst if x not in valLst]
    colLst.append(str(dicInput[ef.Inputs.FlagName]))
    
    lylRdd = rdd.filter(f.col(dicInput[ef.Inputs.Values][0]) == 1).withColumnRenamed('household_id', 'hhid') \
    .withColumn('percent_loyalty', f.col(str(dicInput[ef.Inputs.Values][0])+'_sale')/f.col(str(dicInput[ef.Inputs.Values][1])+'_sale'))
    
    intermediateLylRdd = lylRdd.select(['hhid', 'percent_loyalty']).withColumn(dicInput[ef.Inputs.FlagName], when((f.col('percent_loyalty') >= 0.0) & (f.col('percent_loyalty') <= 0.30), f.lit('Non-loyals')) \
    .when((f.col('percent_loyalty') > 0.30) & (f.col('percent_loyalty') <= 0.70), f.lit('Switchers')).when((f.col('percent_loyalty') > 0.70) & (f.col('percent_loyalty') <= 1.00), f.lit('Loyals')).otherwise(None))
    
    fnllylRdd = rdd.join(intermediateLylRdd, rdd.household_id == intermediateLylRdd.hhid, 'left').select(colLst)
    
    fnllylRdd.registerTempTable('CrossTab')
    
    del lylRdd, intermediateLylRdd
    
    
##Function to Merge runtime tables, and store the resulting table to temporary table
def mergingTbl(rddToJoin, flgName, hc, lylflgName = None):
    if lylflgName == None:
        rddToJoin.registerTempTable('TempHHTable')
        query = "SELECT a.*, b.{} from CrossTab a Left join TempHHTable b on a.household_id = b.hh_id".format(flgName)
        ss = hc.sql(query)
        ss.registerTempTable('CrossTab')
        del ss, query
    else:
        rddToJoin.registerTempTable('TempHHTable')
        query = "SELECT a.*, b.{0}, b.{1} from CrossTab a Left join TempHHTable b on a.household_id = b.hh_id".format(flgName, lylflgName)
        ss = hc.sql(query)
        ss.registerTempTable('CrossTab')
        del ss, query

    
##Function to generate period to be used in where conditions
def periodCalc(enddate, period):
    return last_day_of_month((datetime.datetime.strptime(enddate,"%Y-%m-%d") 
    - dateutil.relativedelta.relativedelta(months = int(period))).date()) \
    + dateutil.relativedelta.relativedelta(days = 1)
    
def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


