# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 11:25:52 2017

@author: vn0bz25
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
from BinFlagFunctions import funcUpcForBins, funcSubCatForBins
import BinFlagFunctions as cd
import EnumFile as ef
import pandas as pd
import sys
import gc

scanTbl = sys.argv[1]
bucktLst = sys.argv[4:]
print("Input Bins are...")
print(bucktLst)

#1to3 3to4 5to8 8to10 10+

ff = pd.read_csv("BinFlagInput.csv", keep_default_na = False, na_filter = False)
keyNames = [ef.Inputs.FlagName, ef.Inputs.Operation, ef.Inputs.FilterClause, ef.Inputs.TimePeriod, ef.Inputs.EndDate, ef.Inputs.Values]
dicInput = []
for items in ff:
    dicInput.append(dict(zip(keyNames, cd.converting_to_dict(items,ff[items]))))

sc = SparkContext()    
hc = HiveContext(sc)
##"""Referencing scan table"""   
hc.sql('USE CKPIRI')
query = "SELECT * FROM CKPIRI.{}".format(scanTbl)
tblScan = hc.sql(query)
del query, scanTbl

funcMap = { "subcat" : funcSubCatForBins,
            "upc" : funcUpcForBins
          }

udffunctoBin = udf(lambda c: cd.functoBin(c, bucktLst), StringType())

xTabName = sys.argv[2]

for i in range(len(dicInput)):
    rr = funcMap[dicInput[i][ef.Inputs.FilterClause]](tblScan, dicInput[i], bucktLst, udffunctoBin)
    rr.registerTempTable('CrossTab')
    print("Merging")
    query = "SELECT a.*, b.{} from {} a Left join CrossTab b on a.household_id = b.hh_id".format(dicInput[i][ef.Inputs.FlagName], xTabName)
    ss = hc.sql(query)
    print("Merging Completed....")
    ss.write.mode("overwrite").saveAsTable(sys.argv[3],format = "orc")
    print("Updated new_%s Table loaded in CKPIRI...." %xTabName)
    query = "Drop Table if exists CKPIRI.{}".format(xTabName)
    hc.sql(query)
    print("Old %s Dropped...." %xTabName)
    gc.collect()
    
del bucktLst, ff, dicInput, keyNames
sc.stop()