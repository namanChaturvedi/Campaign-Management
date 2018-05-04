# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 16:05:31 2017

@author: vn0bz25
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
import sys
import gc

xtabName = sys.argv[1]
ModelDecileTblName = sys.argv[2]
wmtDecileTblName = sys.argv[3]
new_xtabName = sys.argv[4]

sc = SparkContext()    
hc = HiveContext(sc)
hc.sql('USE CKPIRI')

print("Merging Table")
query = "SELECT a.*, b.cat_Wmshare_Decile, b.cat_Wmspend_Decile, b.cat_Decile, c.WMDecile from {} a Left join {} b on a.household_id = b.household_id Left join {} c on a.household_id = c.household_id" \
.format(xtabName, ModelDecileTblName, wmtDecileTblName)
print("Merging Completed")
ss = hc.sql(query).fillna(0)
print("Writing started to table %s in CKPIRI" %new_xtabName)
ss.write.mode("overwrite").saveAsTable(new_xtabName,format = "orc")
print("Writing completed, dropping old %s table..." %xtabName)
query = "Drop table if exists CKPIRI.{}".format(xtabName)
hc.sql(query)
print("Operations Completed, code terminating successfully !")
del xtabName, ModelDecileTblName, wmtDecileTblName, new_xtabName
gc.collect()
sc.stop()