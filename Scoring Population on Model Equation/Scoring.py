# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 11:56:36 2017
Module - Scoring
Description - Scores Only Buyers on Modelled equation 
@author: vn0bz25
"""
#Importing required Libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.types as typ
import pyspark.sql.functions as func
import pandas as pd
from pyspark import StorageLevel
import importlib
import gc
import MappingScript as ms
import traceback
import sys

#This class contains function for merging variables generated in runtime, scoring the dataset, 
#and performing missing value treatment for demo variables
class codeValidation:
    def __init__(self):
        print("INPUT SHEET VALIDATION IN PROGRESS TO CHECK FOR VALID INPUT VALUES")
        
    def isNaN(self,num):
        return num !=num
    
    def functoValidate(self, inputSheet):
        assert inputSheet.index[0] == 'intercept', "Value for 'intercept' is case sensitive and needs to be passed in lowercase only, please check and rerun!"
        if inputSheet.loc['intercept']['operation'].lower() not in ('all','buyer','nonbuyer'):
            print("Pass valid intercept operation i.e. Any(all, buyer, nonbuyer)")
            return False
        if not all(inputSheet['operation'].str.lower().isin(['all','buyer','nonbuyer','totqty', 'avgsaleitem', 'avgsaletx','hasshop','mosales','sale','totitem','tottx','unqitem','demo', 'custom']).tolist()):
            print("Input operation value(s) not matching valid set of operations allowed to enter. Pass Valid operation name across column 'operation'")
            print("ALLOWED SET OF VALUES ARE....")
            print("all, buyer, nonbuyer, totqty, avgsaleitem, avgsaletx, hasshop, mosales, sale, totitem, tottx, unqitem, demo, custom")
            return False
        if not all([self.isNaN(x) for x in inputSheet['rangecustomvar'].tolist()]):
            if not all(pd.core.series.Series(x for x in inputSheet['rangecustomvar'].str.lower() if not self.isNaN(x)).isin(['age','demo','rangeincome']).tolist()):
                print("Pass Valid RANGECUSTOMVAR name, should be Any(age, demo, rangeincome)")
                return False
            else:
                print("INPUT SHEET VALIDATION SUCCESSFUL !!")
                return True                
        else:
            print("INPUT SHEET VALIDATION SUCCESSFULL !!")
            return True
     
class merging:
    def __init__(self, intercept, operation):
        self.intercept = intercept
        self.opr = operation

#function defination for table join for variables being generated.
    def mergingTbl(self,dfToJoin, colName, hc, wmFlg = False):
        dfToJoin.registerTempTable('TempHHTable')
        if not wmFlg:
            query = "SELECT a.*, b.{} from scoring_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName)
            ss = hc.sql(query)
            ss.registerTempTable('scoring_temp_table')
        else:
            query = "SELECT a.*, b.{}, b.Total_Sales from scoring_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName)
            ss = hc.sql(query)
            {
                'buyer' : self.funcForBuyerFiltering,
                'nonbuyer' : self.funcForNonBuyerFiltering,
                'all' : self.funcForAllFiltering
            }[self.opr.lower()](ss)
        hc.dropTempTable('TempHHTable')
        del ss, query
        gc.collect()
        
    def funcForBuyerFiltering(self, t):
        print ("Filtering HH for Buyer Model Scoring")
        t.fillna(0).filter(func.col('Total_Sales')>0).registerTempTable('scoring_temp_table')
        
    def funcForNonBuyerFiltering(self, t):
        print ("Filtering HH for Non Buyer Model Scoring")
        t.fillna(0).filter(func.col('Total_Sales')<=0).registerTempTable('scoring_temp_table')
        
    def funcForAllFiltering(self, t):
        print ("Filtering HH for Scoring on whole population")
        t.registerTempTable('scoring_temp_table')

#function defination for scoring, this code evaluates TotalMarketScore, WMShareScore, and WMSpendScore across households            
    def Scoring(self, hc):
        tbl = hc.sql('select * from scoring_table').fillna(0)
        ss = tbl.rdd.map(tuple).map(lambda x:(x[0],  sum(x[1:len(x)-1]), x[len(x)-1]))\
        .map(lambda x:(x[0], x[1]+self.intercept, x[2], x[2]/(x[1]+self.intercept)))\
        .toDF(["household_id", "TotalMarketSpendScore", "WMSpendScore", "WMShareScore"])
        ss.registerTempTable('final_scoring_temp_table')
        hc.dropTempTable('scoring_table')
        del tbl, ss
        gc.collect()

#functions defination for identifying missing value treatment (if any) for demographic variables        
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
    
    def quantile(self,tbl, key, coef, sc):
        uniqueItems = tbl.distinct().rdd.map(lambda x: x[0]).collect()
        print uniqueItems
        if len(uniqueItems) != 1:
            rddSortedWithIndex = tbl.map(tuple).map(lambda x: int(x[0]/coef)).sortBy(lambda x: x).zipWithIndex().\
                map(lambda (x, i): (i, x)).persist(StorageLevel.MEMORY_AND_DISK)
            n = self.medianOpr(tbl.count())
            print n
            print type(n)
            print "generated count"
            if bool(n):
                broadcastVar = sc.broadcast(n)
                print type(broadcastVar.value)
                print "created broadcast var"
                medianLst = rddSortedWithIndex.filter(lambda row: row[0] in broadcastVar.value).map(lambda x:x[1]).collect()
                print medianLst
                if len(medianLst) == 2:
                    del rddSortedWithIndex
                    print "identified even values of median, and division"
                    return sum(medianLst)/2
                else:
                    print "identified odd values of median"
                    del rddSortedWithIndex
                    return medianLst[0]
            else:
                del rddSortedWithIndex
                return 0
        else:
            return uniqueItems[0]/coef

#function to identify and remove Nan while reading excel sheets.        
def isNaN(num):
    return num !=num

#Setting up Spark and Hive Contexts
def main(argv):
    try:
        assert len(argv) >=2, "Command Line arguments passed not meeting minimum criteria of TableName and ScanTableName !"
        sc = SparkContext(appName=argv[1])    
        hc = HiveContext(sc)
        objCodeValidation = codeValidation()
        if not objCodeValidation.functoValidate(pd.read_excel("ScoringInput.xlsx")):
            raise ValueError('Something not right in ScoringInput.xlsx sheet, please check....Code terminating')
        del objCodeValidation
        gc.collect()
    #   Setting up database
        hc.sql('USE CKPIRI')
        query = "select * from {}".format(argv[2])
        tbl_scan = hc.sql(query)
    #   Identifying distinct Households to be considered for scoring
        tbl_scan.select(tbl_scan.household_id).distinct().registerTempTable('scoring_temp_table')
               
         #Reading Input file
        dicInput = pd.read_excel("ScoringInput.xlsx").T.to_dict()
        
        objMerging = merging(dicInput['intercept']['coefficient'],str(dicInput['intercept']['operation']).lower())
        del dicInput['intercept']
        #Adding Column Name to dictionary object in order to retain the exact names provided by the user in input sheet
        for items in dicInput:
            dicInput[items]['variableName'] = items
            
        #Importing scoring function library, which contains classes corresponding to unique operations required to generate respective variables
        my_module = importlib.import_module("ScoringFunctions")
        dic_wmspend_var = {}
        keys_for_wmspend_var = ()
        #Iterating through each row in input sheet, and identifying operations to be applied in order to generate the required variable.
        for items in dicInput:
            gc.collect()
            clean_dict = {k: dicInput[items][k] for k in dicInput[items] if not isNaN(dicInput[items][k])}
            k_item_for_map = tuple(x for x in clean_dict.keys() if x not in ['wmspend','rangecustomvar', 'enddate', 'operation','coefficient', 'variableName', 'timeperiod'])
            if not bool(clean_dict['wmspend']):
                #Code from here process variables where wmspend flag in Input sheet is 0....
                oprClass = getattr(my_module, str(clean_dict["operation"]).lower())
                if str(clean_dict["operation"]).lower() != "demo":
                    #Code from here process only BEHAVIORIAL variables......
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
                #Code from here process variables where wmspend flag in Input sheet is 1....
                dic_wmspend_var = clean_dict
                keys_for_wmspend_var = k_item_for_map
                gc.collect()
        
        #Processing final column for which user has provided WMSpend Flag as 1...
        print("Generating WMSpend variable %s for Scoring...."%dic_wmspend_var['variableName'])
        oprClass = getattr(my_module, str(dic_wmspend_var["operation"]).lower())
        obj_oprClass = oprClass(tbl_scan,ms.keysMapping[keys_for_wmspend_var], dic_wmspend_var)
        objMerging.mergingTbl(obj_oprClass.operation(obj_oprClass), dic_wmspend_var["variableName"], hc, bool(dic_wmspend_var["wmspend"]))
        del tbl_scan
        gc.collect()
        
        mergedTbl = hc.sql('SELECT * FROM scoring_temp_table').cache()#(StorageLevel.MEMORY_AND_DISK)
        dicInput = pd.read_excel("ScoringInput.xlsx")
        keys_for_missing = objMerging.identifyColforMissingTreatment(dicInput)
        print("column(s) required MEDIAN treatment : %s.."%keys_for_missing)
        
        imputeDf = mergedTbl
        if len(keys_for_missing) != 0:
            for item in keys_for_missing:
                coeff = float(dicInput.loc[item]['coefficient'])
                medianVal = objMerging.quantile(mergedTbl.select(item).na.drop(),item,coeff,sc)
                print("MEDIAN VALUE FOR %s is %s"%(item, medianVal))
                imputeDf = imputeDf.na.fill(medianVal*coeff,[item]).cache()
                gc.collect()
        
        del mergedTbl
#        imputeDf.write.mode("overwrite").saveAsTable("nc_test", format="orc")
        imputeDf.registerTempTable('scoring_table')
        hc.dropTempTable('scoring_temp_table')
        objMerging.Scoring(hc)
        del imputeDf
        gc.collect()
        
        #Processing final Table containing TotalMarketSpendScore, WMSpendScore, WMShareScore and loading into CKPIRI...
        finalTbl = hc.sql('SELECT * FROM final_scoring_temp_table').fillna(0)
        print("Merging all columns and loading table %s in CKPIRI..."%argv[1])
        finalTbl.write.mode("overwrite").saveAsTable(argv[1],format = "orc")
        print("%s table created, code terminating successfully !"%argv[1])
        hc.dropTempTable('final_scoring_temp_table')
    
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