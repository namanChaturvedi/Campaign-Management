# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 14:14:26 2017

@author: vn0bz25
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pandas as pd
import gc
import os
import sys
import traceback

#Setting up Spark and Hive Contexts
def main(argv):
    try:
        assert len(argv) >=1, "Command Line arguments passed not meeting minimum criteria of Experian Table Name !"
        sc = SparkContext(appName="Tracking Report Generation")    
        hc = HiveContext(sc)
        pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
        hc.sql('USE CKPIRI')
              
        prefix = argv[1]
        lsTableNames = (prefix+'_pre_missing_VarSummary', prefix+'_post_missing_VarSummary', prefix+'_1_summary', prefix+'_2_summary',\
        prefix+'_3_summary')
        lsMessages = ("Pre Missing Report", "Post Missing Report", "Walmart Spend Decile Level Report",\
        "Total Market Spend Decile Level Report", "Walmart Share Decile Level Report")
        
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
            
        del lsTableNames, lsMessages
        
        for key in dicTable:
            query = "select * from {0}".format(key)
            tbl = hc.sql(query).toPandas()
            tbl.to_excel(key+'.xlsx', index = False)
            print "{0}.xlsx AS {1}".format(key, dicTable[key])
            del tbl, query
            gc.collect()
        
        print "\nProgram Terminating Successfully !!!!"
        sc.stop()
    except:
        print("ERROR KEY: %s" %sys.exc_info()[0])
        print("ERROR: %s" %sys.exc_info()[1])
        print("TRACEBACK: %s" %sys.exc_info()[2])
        traceback.print_exception(*sys.exc_info())
        
    finally:
        gc.collect()
        sc.stop()
        
if __name__ == "__main__":
    main(sys.argv)