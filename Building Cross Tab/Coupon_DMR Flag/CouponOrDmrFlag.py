# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 11:12:45 2017

@author: vn0bz25
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import *
import sys
import gc

cORdTbl = sys.argv[2]
xTabTbl = sys.argv[3]
colNameforCORdFlg = sys.argv[4]
newxTabTbl = sys.argv[5]
dmrTbl = sys.argv[6]
colNameforDmr = sys.argv[7]

sc = SparkContext()    
hc = HiveContext(sc)
hc.sql('USE CKPIRI')

if sys.argv[1].lower() == "both":
    print("Merging Table")
    query = "SELECT a.*, b.{}, c.{} from {} a Left join {} b on a.household_id = b.household_id Left join {} c on a.household_id = c.household_id" \
    .format(colNameforCORdFlg, colNameforDmr, xTabTbl, cORdTbl, dmrTbl)
    print("Merging Completed")
    ss = hc.sql(query).fillna(0)
    print("Writing started to table %s in CKPIRI" %newxTabTbl)
    ss.write.mode("overwrite").saveAsTable(newxTabTbl,format = "orc")
    print("Writing completed, dropping old %s table..." %xTabTbl)
    query = "Drop table if exists CKPIRI.{}".format(xTabTbl)
    hc.sql(query)
    print("Operations Completed, code terminating successfully !")
    
    del ss, cORdTbl, xTabTbl, colNameforCORdFlg, newxTabTbl, query, dmrTbl, colNameforDmr 
    
elif sys.argv[1].lower() == "or":
    print("Merging Table")
    query = "SELECT a.*, b.{}, c.{} from {} a Left join {} b on a.household_id = b.household_id Left join {} c on a.household_id = c.household_id" \
    .format(colNameforCORdFlg, colNameforDmr, xTabTbl, cORdTbl, dmrTbl)
    print("Merging Completed")
    ss = hc.sql(query).fillna(0)
    ss.withColumn('coupon_or_dmr', when((col(colNameforCORdFlg) > 0) | (col(colNameforDmr) > 0), 1).otherwise(0)).write.mode("overwrite").saveAsTable(newxTabTbl,format = "orc")
    print("Writing completed, dropping old %s table..." %xTabTbl)
    query = "Drop table if exists CKPIRI.{}".format(xTabTbl)
    hc.sql(query)
    print("Operations Completed, code terminating successfully !")
    
    del ss, cORdTbl, xTabTbl, colNameforCORdFlg, newxTabTbl, query, dmrTbl, colNameforDmr

else:
    print("Merging Table")
    query = "SELECT a.*, b.{} from {} a Left join {} b on a.household_id = b.household_id" \
    .format(colNameforCORdFlg, xTabTbl, cORdTbl)
    print("Merging Completed")
    ss = hc.sql(query).fillna(0)
    print("Writing started to table %s in CKPIRI" %newxTabTbl)
    ss.write.mode("overwrite").saveAsTable(newxTabTbl,format = "orc")
    print("Writing completed, dropping old %s table..." %xTabTbl)
    query = "Drop table if exists CKPIRI.{}".format(xTabTbl)
    hc.sql(query)
    print("Operations Completed, code terminating successfully !")
    
    del ss, cORdTbl, xTabTbl, colNameforCORdFlg, newxTabTbl, query
gc.collect()
sc.stop()

    