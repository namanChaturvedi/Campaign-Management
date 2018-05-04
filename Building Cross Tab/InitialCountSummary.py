# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 15:33:18 2017

@author: vn0bz25
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import col
from pyspark.sql.types import *
import pandas as pd
import sys
import gc

sc = SparkContext()    
hc = HiveContext(sc)
##"""Referencing scan table"""   
hc.sql('USE CKPIRI')
query = "SELECT * FROM CKPIRI.{}".format(sys.argv[1])
finalTbl = hc.sql(query)
finalTbl.groupby(list(finalTbl.schema.names[1:])).count() \
.withColumn('Adjusted_HH',(col('count')*0.54).cast(IntegerType())) \
.write.mode("overwrite").saveAsTable('summary_'+sys.argv[1],format = "orc")
print("Initial count summary created and saved in CKPIRI as summary_%s"%sys.argv[1])
print("Code terminating successfully !")
del query, finalTbl
gc.collect()
sc.stop()
