# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 11:56:36 2017
Description - This module Generates Variables and Loading the table in CKPIRI. Publishing variable summary in current working directory.
@author: vn0bz25
"""
#Importing required Libraries
from pyspark import SparkContext, SparkConf
from pyspark import StorageLevel
from pyspark.sql import HiveContext
import pyspark.sql.types as typ
import pyspark.sql.functions as func
import pandas as pd
import time
import importlib
import gc
import MappingScript as ms
import sys

class codeValidation:
    def __init__(self):
        print("INPUT SHEET VALIDATION IN PROGRESS TO CHECK FOR VALID INPUT VALUES\n")
        
    def isNaN(self,num):
        return num !=num
    
    def functoValidate(self, inputSheet):
        assert inputSheet.index[0] == 'intercept', "Value for 'intercept' is case sensitive and needs to be passed in lowercase only, please check and rerun!"
        if inputSheet.loc['intercept']['operation'].lower() not in ('all','buyer','nonbuyer', 'banner'):
            print("Pass valid intercept operation i.e. Any(all, buyer, nonbuyer, banner)\n")
            return False
        if not all(inputSheet['operation'].str.lower().isin(['all','buyer','nonbuyer','totqty', 'pctsale', 'avgsaleitem','wkslsttx', 'avgsaletx','hasshop','mosales','sale','totitem','tottx','unqitem','demo', 'custom']).tolist()):
            print("Input operation value(s) not matching valid set of operations allowed to enter. Pass Valid operation name across column 'operation'\n")
            print("ALLOWED SET OF VALUES ARE....\n")
            print("all, buyer, nonbuyer, totqty, pctsale, avgsaleitem, avgsaletx, wkslsttx, hasshop, mosales, sale, totitem, tottx, unqitem, demo, custom")
            return False
        if not all([self.isNaN(x) for x in inputSheet['rangecustomvar'].tolist()]):
            if not all(pd.core.series.Series(x for x in inputSheet['rangecustomvar'].str.lower() if not self.isNaN(x)).isin(['age','demo','rangeincome']).tolist()):
                print("Pass Valid RANGECUSTOMVAR name, should be Any(age, demo, rangeincome)\n")
                return False
            else:
                print("INPUT SHEET VALIDATION SUCCESSFUL !!\n")
                return True
        if sum(inputSheet['wmspend'].tolist()[1:]) != 1:
            print("<WMSpend variable missing> OR <User has passed more than one WMSpend variable>")
            print("Declare any Sales variable as wmspend variable, Make sure there is only one wmspend variable in ScoringInput Sheet")
            return False
        else:
            print("INPUT SHEET VALIDATION SUCCESSFULL !!\n")
            return True


class merging:
    def __init__(self, intercept, operation):
        self.intercept = intercept
        self.opr = operation
        
    def mergingTbl(self,dfToJoin, colName, hc, wmFlg = False):
        dfToJoin.registerTempTable('TempHHTable')
        if not wmFlg:
            query = "SELECT a.*, b.{} from scoring_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName)
            ss = hc.sql(query)
            if 'wkslsttx' in colName.lower():
                ss = ss.na.fill({colName : 52})
            else:
                pass #Any other condition can be written here for any variables other than wkslsttx
            ss.registerTempTable('scoring_temp_table')
        else:
            query = "SELECT a.*, b.{}, b.Total_Sales from scoring_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName)
            ss = hc.sql(query)
            {
                'buyer' : self.funcForBuyerFiltering,
                'nonbuyer' : self.funcForNonBuyerFiltering,
                'all' : self.funcForAllFiltering,
                'banner' : self.funcForAllFiltering
            }[self.opr.lower()](ss)
        hc.dropTempTable('TempHHTable')
        del ss, query
        gc.collect()
        
    def funcForBuyerFiltering(self, t):
        print ("Filtering HH for Buyer Model Scoring")
        t.filter(func.col('Total_Sales')>0).registerTempTable('scoring_temp_table')
        
    def funcForNonBuyerFiltering(self, t):
        print ("Filtering HH for Non Buyer Model Scoring")
        t.fillna(0).filter(func.col('Total_Sales')<=0).registerTempTable('scoring_temp_table')
        
    def funcForAllFiltering(self, t):
        print ("Filtering HH for Scoring on whole population")
        t.registerTempTable('scoring_temp_table')
           
    def count_null(self,c):
        return func.sum(func.col(c).isNull().cast("integer")).alias(c)

    def sum_not_null(self,c):
        return func.sum(func.col(c).cast("double")).alias(c)
        
    def csvTohive(self, dfFile, strPrint, tblName):
        dfFile.fillna(0).write.mode('overwrite').saveAsTable(tblName,format='orc')
        print "%s created as %s"%(strPrint, tblName)        
        
    def summary(self,tbl,filename,event,hc):
        exprs = [self.count_null(c) for c in tbl.columns]
        null_count_summary = tbl.agg(*exprs).toPandas().T
        null_count_summary.columns = pd.core.index.Index(['missing_count'])
        exprs = [self.sum_not_null(c) for c in tbl.columns]
        col_sum_summary = tbl.agg(*exprs).toPandas().T
        col_sum_summary.columns = pd.core.index.Index(['sum'])
        col_summary = tbl.describe().toPandas().T
        col_summary.columns = pd.core.index.Index(list(col_summary.loc['summary']))
        self.csvTohive(hc.createDataFrame((null_count_summary.join(col_sum_summary,how = 'left').join(col_summary,how = 'left')).reset_index()),event, filename+'_VarSummary')
#        print("Variable summary generated successfully check for %s in current working directory"%(filename+'_VarSummary.xlsx'))
#        null_count_summary.join(col_sum_summary,how = 'left').join(col_summary,how = 'left').to_excel(filename+'_varSummary.xlsx')
        del exprs,null_count_summary, col_sum_summary, col_summary
        gc.collect()
        
    def identifyColforMissingTreatment(self,inputSheet):
        l = inputSheet[inputSheet['operation']=='demo'].index.tolist()
        missingVal_sheet = pd.read_excel("Missing_value.xlsm")[['var_name', 'Missing_Value']].set_index(['var_name'])
        #columns required treatment will be filtered to filteredCols_final
        filteredCols_final = missingVal_sheet.loc[l][missingVal_sheet.loc[l]['Missing_Value'] == 'Median'].index.tolist()
        del l, missingVal_sheet
        gc.collect()
        return filteredCols_final
        
    def medianOpr(self,c):
        if c < 1:
            return None
        if c % 2 == 1:
            return [c//2]
        else:
            return [c//2-1,c//2+1]
    
    def quantile(self,rdd, key, sc):
        uniqueItems = rdd.distinct().rdd.map(lambda x: x[0]).collect()
        if len(uniqueItems) != 1:
            rddSortedWithIndex = (rdd.sort(func.asc(key)).rdd.map(tuple).zipWithIndex().\
                map(lambda (x, i): (i, x)).cache())
            n = self.medianOpr(rdd.count())
            if bool(n):
                broadcastVar = sc.broadcast(n)
                medianLst = rddSortedWithIndex.filter(lambda row: int(row[0] in broadcastVar.value)).map(lambda x:x[1][0]).collect()
                if len(medianLst) == 2:
                    return sum(medianLst)//2
                else:
                    return medianLst[0]
            else:
                return 0
        else:
            return uniqueItems[0]
            
        
def isNaN(num):
    return num !=num
    
#Setting up Spark and Hive Contexts
def main(argv):
    try:
        sc = SparkContext(conf = SparkConf().setAppName(argv[1]))
        hc = HiveContext(sc)
        hc.sql("set spark.sql.shuffle.partitions = 500")
#        hc.sql("set spark.sql.tungsten.enabled = false")
        objCodeValidation = codeValidation()
        if not objCodeValidation.functoValidate(pd.read_excel("ScoringInput.xlsx")):
            raise ValueError('Something not right in ScoringInput.xlsx sheet, please check....Code terminating\n')
        del objCodeValidation
        hc.sql('USE CKPIRI')
        query = "select * from {}".format(argv[2])
        tbl_scan = hc.sql(query)
        tbl_scan.select(tbl_scan.household_id).distinct().registerTempTable('scoring_temp_table')
         
        
         #Reading Input file
        dicInput = pd.read_excel("ScoringInput.xlsx").T.to_dict()
        objMerging = merging(dicInput['intercept']['coefficient'],str(dicInput['intercept']['operation']).lower()) 
        del dicInput['intercept']
        #Adding Column Name to dictionary object
        for items in dicInput:
            dicInput[items]['variableName'] = items
            
        #Importing scoring function module class 
        my_module = importlib.import_module("ScoringFunctionsForSummary")
        dic_wmspend_var = {}
        keys_for_wmspend_var = ()
        #Calling Functions 
        print "\nWARNING : Please Ensure the key for which Lookup will be made from SingleVars list to demo variable in demo table, should exist. Otherwise, the value of that demo variable will be imputed as per missing value treatment\n"
        for items in dicInput:
            gc.collect()
            clean_dict = {k: dicInput[items][k] for k in dicInput[items] if not isNaN(dicInput[items][k])}
            k_item_for_map = tuple(x for x in clean_dict.keys() if x not in ['wmspend','rangecustomvar', 'enddate', 'operation','coefficient', 'variableName', 'timeperiod'])
            if not bool(clean_dict['wmspend']):
                #        obj_dynamicObjectCreation = dynamicObjectCreation(clean_dict)
                oprClass = getattr(my_module, str(clean_dict["operation"]).lower())
                if str(clean_dict["operation"]).lower() != "demo":
                    print("Generating behaviorial variable %s for Scoring...."%items)
                    obj_oprClass = oprClass(tbl_scan,ms.keysMapping[k_item_for_map], clean_dict)
                    objMerging.mergingTbl(obj_oprClass.operation(obj_oprClass), clean_dict["variableName"], hc, bool(clean_dict["wmspend"]))
                    gc.collect()
                else:
                    #Code from here process only DEMO variables......
                    ###If block just for Range and Custome Age Variable to update valid lookup value in single var sheet
                    if clean_dict['variableName'].lower() in ['is_income_over_50k', 'is_income_over_100k', 'is_income_40k_65k']:
                        lookup_value = 'hh_est_income'
                    else:
                        lookup_value = clean_dict['variableName']
                    gc.collect()
                    print("Generating demo variable %s for Scoring...."%items)
                    dfSingleVars = pd.read_excel("SingleValueVars.xlsx")[['final_var_name', 'Value', 'var_name', 'var_value']].set_index(['final_var_name']).loc[lookup_value]
                    schema = typ.StructType([typ.StructField('var_value',typ.StringType(),True),typ.StructField('Value',typ.DoubleType(),True)])
                    if type(dfSingleVars) == pd.core.series.Series:
                        #This IF Block is to handle data from Single Vars sheet which generates Pandas Series when filtered with final_var_name...
                        query = "select household_id,{} from {}".format(dfSingleVars['var_name'],argv[3])
                        g = dfSingleVars[['var_value','Value']].to_frame().T
                        g['Value'] = g['Value'].astype(float)
                        dfMappingSheet = hc.createDataFrame(g, schema)
                        colName = dfSingleVars['var_name']
                    else:
                        #This ELSE Block is to handle data from Single Vars sheet which generates Pandas Dataframe when filtered with final_var_name...
                        query = "select household_id,{} from {}".format(dfSingleVars['var_name'][0],argv[3])
                        dfMappingSheet = hc.createDataFrame(dfSingleVars.reset_index()[['var_value','Value']], schema)
                        colName = dfSingleVars['var_name'][0]
                    tbl_demo = hc.sql(query)
                    obj_oprClass = oprClass(tbl_demo, dfMappingSheet, clean_dict['rangecustomvar'].lower(), colName, clean_dict)
                    objMerging.mergingTbl(obj_oprClass.operation(obj_oprClass), clean_dict["variableName"], hc, bool(clean_dict["wmspend"]))
                    del query, colName
            else:
                dic_wmspend_var = clean_dict
                keys_for_wmspend_var = k_item_for_map
                gc.collect()
           
        gc.collect()
        oprClass = getattr(my_module, str(dic_wmspend_var["operation"]).lower())
        obj_oprClass = oprClass(tbl_scan,ms.keysMapping[keys_for_wmspend_var], dic_wmspend_var)
        objMerging.mergingTbl(obj_oprClass.operation(obj_oprClass), dic_wmspend_var["variableName"], hc, bool(dic_wmspend_var["wmspend"]))
        keys_for_missing = objMerging.identifyColforMissingTreatment(pd.read_excel("ScoringInput.xlsx"))                

        mergedTbl = hc.sql('SELECT * FROM scoring_temp_table').persist(StorageLevel.MEMORY_AND_DISK_2)

        objMerging.summary(mergedTbl.select([x for x in mergedTbl.schema.names if x not in 'Total_Sales']),argv[1]+'_pre_missing','Pre Missing Summary', hc)
        print("\nMerging all columns and loading raw table %s in CKPIRI..."%(argv[1]))
        mergedTbl.select([x for x in mergedTbl.schema.names if x not in 'Total_Sales'])\
        .fillna(0).write.mode("overwrite").saveAsTable(argv[1]+'_raw',format = "orc")
        print("%s table created without missing treatment!\n"%(argv[1]+'_raw'))
        imputeDf = mergedTbl
        if len(keys_for_missing) != 0:
            for item in keys_for_missing:
                medianVal = objMerging.quantile(mergedTbl.select(item).na.drop(),item,sc)
                print(item, medianVal)
                imputeDf = imputeDf.na.fill(medianVal,[item])
                gc.collect()
                
        objMerging.summary(imputeDf.select([x for x in imputeDf.schema.names if x not in 'Total_Sales']).fillna(0),argv[1]+'_post_missing', 'Post Missing Summary', hc)
        
        print("\nMerging all columns and loading missing values imputed table %s in CKPIRI..."%(argv[1]))
        imputeDf.select([x for x in imputeDf.schema.names if x not in 'Total_Sales'])\
        .fillna(0).write.mode("overwrite").saveAsTable(argv[1]+'_imputed',format = "orc")
        print("%s table created, code terminating successfully !\n"%(argv[1]+'_imputed'))
        hc.dropTempTable('scoring_temp_table')
        sc.stop()
    except:
        print("<p>KEY: %s</p>" %sys.exc_info()[0])
        print("<p>EERROR: %s</p>" %sys.exc_info()[1])
        print("<p>Traceback: %s</p>" %sys.exc_info()[2])
    finally:
        gc.collect()
        sc.stop()

if __name__ == "__main__":
    main(sys.argv)