# -*- coding: utf-8 -*-
"""
Created on Mon Aug 07 13:29:18 2017

@author: vn0bz25
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pandas as pd
import gc
import sys
import traceback

class get_tbl_and_messages:
    def __init__(self, opr, main_tbl, hc):
        if type(opr != str):
            self.operation = [x.lower() for x in opr]
        else:
            self.operation = opr
        self.name = main_tbl
        self.contextObj = hc
    
    def get_tbl_messages(self, localOpr):
        if (localOpr.lower() == "tracking") or (localOpr.lower() == "correlation") or (localOpr.lower() == "conversion") or (localOpr.lower() == "conversionday") or (localOpr.lower() == "scoring") or (localOpr.lower() == "weight"):
            return {
                'tracking' : self.get_tracking_info,
                'correlation' : self.get_correlation_info,
                'conversion' : self.get_conversion_info,
                'conversionday':self.get_conversion_day_info,
                'scoring' : self.get_scoring_info,
                'weight' : self.get_weighted_info
            }[localOpr.lower()](self.name)
        else:
            print "Operation name(s) passed is/are not matching to Valid String\n"
            print "Valid Operations to passed as follows\n"
            print "1) tracking"
            print "2) correlation"
            print "3) conversion"
            print "4) conversionday"
            print "5) scoring"
            print "6) weight"
            
        
    def get_tracking_info(self, strName):
        prefix = '_'.join(strName.split('_')[:-1])
        lsTableNames = (prefix+'_TestflagReport', prefix+'_exp_HH_countReport', prefix+'_cntrl_Adj_HHcount', prefix+'_SegWise_AdjControl',\
        prefix+'_NonAdj_SegTest_report', prefix+'_SegTest_report', prefix+'_Overall_AdjControl', prefix+'_Overall_report', prefix+'_NonAdj_Overall_report')
        lsMessages = ("Test Flag Report", "Experian Household count Report", "Post Segment Adjusted Household Count Report", "Segment Wise Adjusted Control Report",\
        "Non Adjusted Segment wise report containing t-test and chisq test statistics", "Adjusted Segment wise report containing t-test and chisq test statistics",\
        "Overall Adjusted Control Report", "Overall report containing t-test and chisq test statistics", "Non Adjusted Overall wise report containing t-test and chisq test statistics")
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable
        
    def get_correlation_info(self, strName):
        lsTableNames = (strName+'_dept_corr', strName+'_corr')
        lsMessages = ('Department Level Correlation', 'Overall Correlation from TOP department')
        dicTable = {}
        #Creating one to one tablename and message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable

    def get_conversion_info(self, strName):
        prefix = '_'.join(strName.split('_')[:-1])
        query = "select * from {0}".format(prefix+'_trans')
        tbl = self.contextObj.sql(query)
        prod_names = [x.split('_')[1] for x in tbl.columns if 'buyer_advprod' in x.lower()]
        del tbl
        lsTableNames = tuple([prefix+'_conversion_'+x for x in prod_names] + [prefix+'_wtd_conversion_'+x for x in prod_names])
        lsMessages = tuple(['Conversion Report for '+x for x in prod_names]+['Weighted Conversion Report for '+x for x in prod_names])
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable
        
    def get_conversion_day_info(self, strName):
        prefix = '_'.join(strName.split('_')[:-1])
        query = "select * from {0}".format(prefix+'_trans')
        tbl = self.contextObj.sql(query)
        prod_names = [x.split('_')[1] for x in tbl.columns if 'buyer_advprod' in x.lower()]
        del tbl
        lsTableNames = tuple([prefix+'_conv_daywise_'+x for x in prod_names])
        lsMessages = tuple(['Day-wise Conversion breakup for '+x for x in prod_names])
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable
        
    def get_scoring_info(self, strName):
        lsTableNames = (strName+'_1_summary', strName+'_2_summary', strName+'_3_summary', strName+'_pre_missing_VarSummary',\
        strName+'_post_missing_VarSummary')
        lsMessages = ("WMSpend Decile Report", "Total Market Spend Decile Report", "WMShare Decile Report", "Pre missing treatment Variable Summary",\
        "Post missing treatment Variable Summary")
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable
        
    def get_weighted_info(self, strName):
        prefix = '_'.join(strName.split('_')[:-1])
        lsTableNames = (prefix+'_Wtd_SegTest_report')
        lsMessages = ("Weighted Segment wise report containing t-test and chisq test statistics")
        dicTable = {}
        #Creating one to one tablename and Message
        for tbl, msg in zip(lsTableNames, lsMessages):
            dicTable[tbl] = msg
        return dicTable
        
    def get_reports(self, dicTable, hc):
        for key in dicTable:
            query = "select * from {0}".format(key)
            tbl = hc.sql(query).toPandas()
            tbl.to_excel(key+'.xlsx', index = False)
            print "{0}.xlsx AS {1}".format(key, dicTable[key])
            del tbl, query
            gc.collect()
        
#Setting up Spark and Hive Contexts
def main(argv):
    try:
        assert len(argv) >=3, "Command Line arguments to be entered as type_of_reports and Mail_Control_Table !"
        sc = SparkContext(appName="Report Generation")    
        hc = HiveContext(sc)
        pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
        hc.sql('USE CKPIRI')
        
        objInfo = get_tbl_and_messages(argv[2:], argv[1], hc)
        if type(objInfo.operation) != str:
            for item in objInfo.operation:
                dicTable = objInfo.get_tbl_messages(item)
                objInfo.get_reports(dicTable, hc)
        else:
            dicTable = objInfo.get_tbl_messages(objInfo.operation)
            objInfo.get_reports(dicTable, hc)
        
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