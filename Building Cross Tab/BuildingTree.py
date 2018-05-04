# -*- coding: utf-8 -*-
"""
Created on Fri Mar 03 10:33:38 2017

@author: vn0bz25
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import col
from pyspark.sql.types import *
import Functions as cd
from Functions import funcSumOperation, funcSubCatOperation, \
funcComboOperation, funcLoyaltyOperation, funcHoldoutOperation, funcCatOperation, funcUpcQtyOperation, funcDeptOperation, \
funcSubcatTrips
import EnumFile as ef
import pandas as pd
import sys
import gc
#import numpy as np
##"""Reading Input csv file and creating list of dictionaries"""
ff = pd.read_csv("BuildingTreeInput.csv", keep_default_na = False, na_filter = False)
keyNames = [ef.Inputs.FlagName, ef.Inputs.Operation, ef.Inputs.FilterClause, ef.Inputs.TimePeriod, ef.Inputs.EndDate, ef.Inputs.Values]
dicInput = []
for items in ff:
    dicInput.append(dict(zip(keyNames, cd.converting_to_dict(items,ff[items]))))
    
#Create the SparkContext
#conf = (SparkConf()
#         .setAppName("BuildingTree"))
funcMap = { "upc" : funcSumOperation,
            "subcat" : funcSubCatOperation,
            "cat" : funcCatOperation,
            "combo" : funcComboOperation,
            "loyalty" : funcLoyaltyOperation,
            "holdout": funcHoldoutOperation,
            "qtyupc" : funcUpcQtyOperation,
            "dept" : funcDeptOperation,
            "subcattrip" : funcSubcatTrips
          }    

sc = SparkContext()    
hc = HiveContext(sc)
##"""Referencing scan table"""   
hc.sql('USE CKPIRI')
query = "SELECT * FROM CKPIRI.{}".format(sys.argv[1])
tblScan = hc.sql(query)
del query

##"""Creating unique household dataframe later to be used to join flags"""
tblScan.select(col('household_id')).distinct().registerTempTable('CrossTab') 

##"""Creating Flags and merging with unique household Ids"""
for i in range(len(dicInput)):
    if dicInput[i][ef.Inputs.Operation] == 'upc' or dicInput[i][ef.Inputs.Operation] == 'subcat' or dicInput[i][ef.Inputs.Operation] == 'cat' or dicInput[i][ef.Inputs.Operation] == 'qtyupc' or dicInput[i][ef.Inputs.Operation] == 'dept' or dicInput[i][ef.Inputs.Operation] == 'subcattrip':
        if dicInput[i][ef.Inputs.FilterClause] == 'loyalty':
            cd.mergingTbl(funcMap[dicInput[i][ef.Inputs.Operation]](tblScan, dicInput[i]), dicInput[i][ef.Inputs.FlagName], hc, str(dicInput[i][ef.Inputs.FlagName])+'_sale')
        else:
            cd.mergingTbl(funcMap[dicInput[i][ef.Inputs.Operation]](tblScan, dicInput[i]), dicInput[i][ef.Inputs.FlagName], hc)
        print("{0} Flag processing completed".format(dicInput[i][ef.Inputs.FlagName]))
    elif dicInput[i][ef.Inputs.Operation] == "combo" or dicInput[i][ef.Inputs.Operation] == "loyalty":
        funcMap[dicInput[i][ef.Inputs.Operation]](hc.sql('SELECT * FROM CrossTab').fillna(0), dicInput[i], hc)
        print("{0} Flag processing completed".format(dicInput[i][ef.Inputs.FlagName]))
    else:
        cd.mergingTbl(funcMap[dicInput[i][ef.Inputs.Operation]](hc.sql('SELECT household_id from CrossTab'), dicInput[i]), dicInput[i][ef.Inputs.FlagName], hc)
        print("{0} Flag processing completed".format(dicInput[i][ef.Inputs.FlagName]))
    gc.collect()

##"""Generating Initial Count summary and writing table to HDFS"""
hc.dropTempTable('TempHHTable')
#sqlContext.sql('SELECT * FROM CrossTab').show(20)
finalTbl = hc.sql('SELECT * FROM CrossTab').fillna(0)
lstCols = list(finalTbl.columns)
lstFilteredCols = [x for x in lstCols if 'holdout' not in x]
finalTbl_f = finalTbl.withColumn('filtered_col', sum(col(x) for x in lstFilteredCols)).where(~(col('filtered_col') == 0)).select(lstCols)
hc.dropTempTable('CrossTab')
#finalTbl = finalTbl.fillna(0)
print("Merging all flags and loading table %s in CKPIRI..." %sys.argv[2])
finalTbl_f.write.mode("overwrite").saveAsTable(sys.argv[2],format = "orc")
print("%s table created, code terminating successfully !"%sys.argv[2])
#finalTbl.groupby(list(finalTbl.schema.names[1:])).count().show()

#.write.format("csv").option("header","true").save('Initial_Count_summary.csv')
del finalTbl, finalTbl_f
del funcMap
gc.collect()
sc.stop()