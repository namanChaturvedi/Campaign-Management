# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 13:29:18 2017

@author: vn0bz25
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.functions as f
import pandas as pd
import gc
import sys
import ast
import traceback
import datetime
import dateutil.relativedelta
from pyspark import StorageLevel

class codeValidation:
    def __init__(self):
        print("INPUT SHEET VALIDATION IN PROGRESS TO CHECK FOR VALID INPUT VALUES")
        
    def isNaN(self,num):
        return num !=num
    
    def functoValidate(self, inputSheet):
        if not all(inputSheet['operation'].str.lower().isin(['flag']).tolist()):
            print("Pass valid operation i.e. flag")
            return False
        else:
            print("INPUT SHEET VALIDATION SUCCESSFULL !!")
            return True
            
    def periodCalc(self, enddate, period):
        return self.last_day_of_month((datetime.datetime.strptime(enddate,"%Y-%m-%d") \
        - dateutil.relativedelta.relativedelta(months = int(period))).date()) \
        + dateutil.relativedelta.relativedelta(days = 1)
    
    def last_day_of_month(self, any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        return next_month - datetime.timedelta(days=next_month.day)
        
class MiscClass():
    def __init__(self):
        pass
           
    def mergingTbl(self, dfToJoin, colName, hc):
        dfToJoin.registerTempTable('TempHHTable')
        query = "SELECT a.*, b.{} from conversion_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName)
        ss = hc.sql(query)
        ss.registerTempTable('conversion_temp_table')
        hc.dropTempTable('TempHHTable')
        del ss, query
        gc.collect()
        
    def csvTohive(self, dfFile, strPrint, tblName):
        dfFile.fillna(0).write.mode('overwrite').saveAsTable(tblName,format='orc')
        print "\n%s created as %s\n"%(strPrint, tblName)
        gc.collect()
        
    def isNaN(self, num):
        return num !=num
        
    def updInputdictionaryObject(self, dInput):
        unq_upc_ls = []
        for keys in dInput:
            unq_upc_ls = unq_upc_ls + list(ast.literal_eval(dInput[keys]['upc']))
        if len(unq_upc_ls) == len(set(unq_upc_ls)):
            print "Multiple Advertised Product lists provided by the user contain unique products"
        else:
            print "Multiple Advertised Product lists provided contain duplicate products, processing to create unique list"
            dInput = {dInput.keys()[0][:-1]:{key : value for key, value in dInput[dInput.keys()[0]].items()}}
            dInput[dInput.keys()[0]]["upc"] = list(set(unq_upc_ls))            
        return dInput
 
class dynamicObjectCreation:
    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)
            
#Class with functions relevant for Generating Trans variables from Scan Table
class flag(dynamicObjectCreation):
    def __init__(self, tbl_m, clean_dict, col_seg, trck_strt_dt, trck_end_dt, con_strt_dt, hc, prefix):
        self.tbl_merged = tbl_m
        self.operation = self.func['upc']
        self.dynProp = dynamicObjectCreation(clean_dict)
        self.seg_col = col_seg
        self.t_start_dt = trck_strt_dt
        self.t_end_dt = trck_end_dt
        self.c_start_dt = con_strt_dt
        self.hiveCntxt = hc
        self.key_mailcntrl = prefix
        
    def generatePeriodTbl(self):
        all_days = [(self.c_start_dt + datetime.timedelta(days = x)) for x in range((self.t_end_dt - self.c_start_dt).days + 1)]
        diff_days = [((x - self.t_start_dt)/7).days + 1 for x in all_days]
        all_days = [x.strftime("%Y-%m-%d") for x in all_days]
        time_p = pd.DataFrame({"dates": all_days, "period": diff_days})
        
        #Generating Output on screen for time period distribution
        unq_p = time_p.period.unique().tolist()
        min_dt = time_p.groupby('period')['dates'].transform(min).unique().tolist()
        max_dt = time_p.groupby('period')['dates'].transform(max).unique().tolist()
        
        distribution_p = pd.DataFrame({"period":unq_p, "From": min_dt, "To":max_dt})
        print "\nDistribution of Periods Identified as\n"
        print distribution_p[["From", "To", "period"]]
        
        del unq_p, min_dt, max_dt
        gc.collect()
        
        return time_p, distribution_p
        
    def func_weighted(self, totSale):
        query = "select * from {0}".format(self.key_mailcntrl+'_Wtd_Exp')
        tbl_wtd_exp = self.hiveCntxt.sql(query)
        tbl_wtd_exp = tbl_wtd_exp.withColumnRenamed('household_id', 'hhid')
        ls_wtd_ratios = [x for x in tbl_wtd_exp.columns if 'hhid' not in x]
        #Merging totSale with wtd experian table to identify ratios
        joined_tbl = totSale.join(tbl_wtd_exp, totSale.household_id == tbl_wtd_exp.hhid, 'left').select(totSale.columns + ls_wtd_ratios)
        return joined_tbl.withColumn('wtd_segment', f.when(joined_tbl.mail_holdout == 'M', 1.0).otherwise(sum(joined_tbl[x] for x in ls_wtd_ratios)))\
        .select([x for x in joined_tbl.columns if x not in ls_wtd_ratios] + ['wtd_segment'])
        
            
    def func_saleupc(self, bolDayWise = False):
        if type(self.dynProp.upc) == unicode:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
        else:
            upc_list = self.dynProp.upc
        filterSale = self.tbl_merged.withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))
        
        time_p, dist_p = self.generatePeriodTbl()
        period_tbl = self.hiveCntxt.createDataFrame(time_p)
        
        #Joining Filter Upc table, and Period table to identify periods for each HH
        merged = filterSale.join(period_tbl, filterSale.visit_date == period_tbl.dates, 'left').fillna(0)\
        .select('household_id','mail_holdout',self.seg_col,'period', 'upc_present', 'retail_price', 'visit_date')
        
        if not bolDayWise:
            totSale = merged.filter(merged.upc_present == 1).select('household_id','mail_holdout',self.seg_col,'period', 'retail_price') \
                .groupBy('household_id','mail_holdout',self.seg_col,'period').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)")).persist(StorageLevel.MEMORY_AND_DISK)
                
            tbl_wtd = self.func_weighted(totSale.select('household_id','mail_holdout',self.seg_col,'period',str(self.dynProp.variableName.lower())))
            
            totOverallCount = totSale.filter(totSale.period >= 1).filter(f.col(str(self.dynProp.variableName.lower())) > 0)\
                .select('household_id','mail_holdout',self.seg_col).groupBy('mail_holdout',self.seg_col).agg(f.countDistinct(f.col('household_id')))
            
            totOverallCount = totOverallCount.withColumnRenamed('count(household_id)', 'buyer_'+str(self.dynProp.variableName).rsplit('_',1)[1]).toPandas()
            totOverallCount['From'], totOverallCount['To'], totOverallCount['period'] = self.t_start_dt.strftime("%Y-%m-%d"), self.t_end_dt.strftime("%Y-%m-%d"), 'Overall'
                
            totCount = totSale.filter(f.col(str(self.dynProp.variableName.lower())) > 0).select('household_id','mail_holdout',self.seg_col,'period')\
                .groupBy('mail_holdout',self.seg_col,'period').agg({'household_id' : 'count'})
                
            totCount = totCount.withColumnRenamed('count(household_id)', 'buyer_'+str(self.dynProp.variableName).rsplit('_',1)[1]).toPandas()
            
            totCount_wtd = tbl_wtd.filter(f.col(str(self.dynProp.variableName.lower())) > 0).select('mail_holdout',self.seg_col,'period','wtd_segment')\
                .groupBy('mail_holdout',self.seg_col,'period').agg({'wtd_segment' : 'sum'})
                
                
            totCount_wtd = totCount_wtd.withColumnRenamed('sum(wtd_segment)', 'buyer_'+str(self.dynProp.variableName).rsplit('_',1)[1]).toPandas()
            return totCount, totOverallCount.reset_index(), dist_p, totCount_wtd
            
        else:
            totSale = merged.filter(merged.upc_present == 1).select('household_id','mail_holdout',self.seg_col,'visit_date','period', 'retail_price') \
                .groupBy('household_id','mail_holdout',self.seg_col,'visit_date','period').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))
            
            totCount = totSale.filter(f.col(str(self.dynProp.variableName.lower())) > 0).select('household_id','mail_holdout',self.seg_col,'visit_date','period')\
            .groupBy('mail_holdout',self.seg_col,'visit_date','period').agg({'household_id' : 'count'})
        
            totCount = totCount.withColumnRenamed('count(household_id)', 'buyer_'+str(self.dynProp.variableName).rsplit('_',1)[1])
            
            return totCount, dist_p
            
    func = dict(
        upc = func_saleupc,
    )
    
class generateAnalyzerTables():
    def __init__(self, tbl_exp, tbl_trans, t_prefix):
        self.tbl_e = tbl_exp
        self.tbl_t = tbl_trans
        self.creatingAnalyzerTables(t_prefix)
        
    def creatingAnalyzerTables(self, t_prefix):
        ls_final_col = self.tbl_e.columns + self.tbl_t.columns[1:]
        ls_seg = self.tbl_e.select(self.tbl_e.Campaign_Cell_Id).toPandas().iloc[:,0].unique().tolist()
        segments = [x for x in self.tbl_e.columns if ((x.lower().endswith('_t')) or (x.lower().endswith('_c')))]
        #identifying unique segments
        col_n = []
        [col_n.append(z) for z in ['_'.join(y.split('_')[:-2]) for y in segments] if z not in col_n]
        uniq_segments = col_n[:]
        for item in col_n:
            if sum([item in x for x in col_n]) >=2:
                uniq_segments.remove(item)
        if len(ls_seg) == len(uniq_segments):
            for item in ls_seg:
                self.tbl_e.where(self.tbl_e.Campaign_Cell_Id == item).join(self.tbl_t, self.tbl_e.Household_Id == self.tbl_t.hhid, 'left').select(ls_final_col)\
                .write.mode("overwrite").saveAsTable(t_prefix+'_'+'_'.join(item.split('_')[1:]), format = "orc")
                print "Table saved for {0} segment as {1}".format('_'.join(item.split('_')[1:]), t_prefix+'_'+'_'.join(item.split('_')[1:]))
                
        else:
            print "\nUnique Segments Identified by Code not matching to unique values in Campaign Cell Id"
            
#Setting up Spark and Hive Contexts
def main(argv):
    try:
        assert len(argv) >=4, "Pass minimum required inputs as MailControlTableName, ScanTableName, StartingDate<YYYY-MM-DD>, yes/no/blank(yes)/day for analyzer table !"
        
        pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
        
        dicInput = pd.read_excel("TrackingInput.xlsx")
        objCodeValidation = codeValidation()
        if not objCodeValidation.functoValidate(dicInput):
            raise ValueError('Something not right in TrackingInput.xlsx sheet, please check....Code terminating')
        del objCodeValidation
        gc.collect()
        
        prefix = '_'.join(argv[1].split('_')[:-1])
        
        sc = SparkContext(appName="Conversion report for "+prefix+" Campaign")   
        hc = HiveContext(sc)
        hc.sql('USE CKPIRI')        
        
        objMiscClass = MiscClass()
        
        if len(argv) == 4:
            flg4Analyzer = 'yes'
        elif (len(argv) > 4) & (argv[4].lower() == 'day'):
            print "\n4th argument passed as {0} to generate Day-wise breakup\n".format(argv[4])
            flg4Analyzer, flg4Daywise = 'no', True
        else:
            flg4Analyzer, flg4Daywise = argv[4].lower(), False
        
        dicInput = pd.read_excel("TrackingInput.xlsx").T.to_dict()
        #Deleting products other than AdvProd
        keys_to_del = [x for x in dicInput.keys() if 'advprod' not in x.rsplit('_',1)[1].lower()]
        for item in keys_to_del:
            del dicInput[item]
            
        if len(dicInput.keys()) > 1:
            print "\nModule found multiple Advertised Product List. Processing to create one unique list"
            dicInput = objMiscClass.updInputdictionaryObject(dicInput)
        else:
            print "\nModule found only one Advertised Products lists"
        #Updating Startdate as per input 
        tracking_start_dt = dicInput[dicInput.keys()[0]]['startdate'].date()
        conversion_start_dt = datetime.datetime.strptime(argv[3],"%Y-%m-%d").date()
        tracking_end_dt = dicInput[dicInput.keys()[0]]['enddate'].date()
        for item in dicInput.keys():
            dicInput[item]['startdate'] = datetime.datetime.strptime(argv[3],"%Y-%m-%d").date()
            dicInput[item]['variableName'] = item
    
            
        query = "select * from {0}".format(prefix+'_Exp')
        tbl_exp = hc.sql(query)
        col_segment = str([x for x in tbl_exp.columns if 'segment' in x.lower()][0])
        del query, tbl_exp
        gc.collect()
        
        query = "select household_id, mail_holdout, {0} from {1}".format(col_segment, prefix+'_Exp')
        tbl_exp = hc.sql(query)
            
        tbl_exp.registerTempTable('conversion_temp_table')
        del query
        gc.collect()
        
        query = "select household_id, visit_date, retail_price, upc_nbr from {0}".format(argv[2])
        #Creating Scan table instance
        tbl_s = hc.sql(query)
        tbl_s = tbl_s.withColumnRenamed('household_id', 'hhid')
        
        #joining experian and scan table
        joinTbl = tbl_s.filter(tbl_s.visit_date.between(conversion_start_dt,tracking_end_dt.strftime('%Y-%m-%d')))\
        .join(tbl_exp, tbl_s.hhid == tbl_exp.household_id, 'inner')\
        .select('household_id','mail_holdout',col_segment,'visit_date', 'upc_nbr', 'retail_price')
        
            
        for items in dicInput:
            gc.collect()
            clean_dict = {k: dicInput[items][k] for k in dicInput[items] if not objMiscClass.isNaN(dicInput[items][k])}
    
            print("Generating sales flag %s for Conversion Report...."%(items))
            obj_oprClass = flag(joinTbl, clean_dict, col_segment, tracking_start_dt, tracking_end_dt, conversion_start_dt, hc, prefix)
            
            if not flg4Daywise:
                tbl_filtered, tot_overall, dist_p, tbl_filtered_wtd = obj_oprClass.operation(obj_oprClass)
                
                
                prod_name = items.rsplit("_",1)[1]
                
                temp_tbl = tbl_filtered.merge(dist_p)[['From', 'To', 'period', 'mail_holdout', col_segment, str('buyer_'+prod_name)]]
                
                tot_overall = tot_overall.pivot_table(index = ['From', 'To', 'period', 'mail_holdout'], columns = col_segment, values = 'buyer_'+prod_name).reset_index()
                
                pivoted_tbl = temp_tbl.pivot_table(index = ['From', 'To', 'period', 'mail_holdout'], columns = col_segment, values = 'buyer_'+prod_name).reset_index()
                
                final_tbl = pd.concat([pivoted_tbl, tot_overall])
                
                final_tbl['period'] = final_tbl['period'].astype(str)
                
                temp_tbl_wtd = tbl_filtered_wtd.merge(dist_p)[['From', 'To', 'period', 'mail_holdout', col_segment, str('buyer_'+prod_name)]]
                pivoted_tbl_wtd = temp_tbl_wtd.pivot_table(index = ['From', 'To', 'period', 'mail_holdout'], columns = col_segment, values = 'buyer_'+prod_name).reset_index()
                final_tbl_wtd = pd.concat([pivoted_tbl_wtd, tot_overall])
                final_tbl_wtd['period'] = final_tbl_wtd['period'].astype(str)
                
                objMiscClass.csvTohive(hc.createDataFrame(final_tbl), "Normal Conversion Table Stored for "+prod_name, prefix+"_conversion_"+prod_name)
                objMiscClass.csvTohive(hc.createDataFrame(final_tbl_wtd), "Weighted Conversion Table Stored for "+prod_name, prefix+"_wtd_conversion_"+prod_name)
    #            objMiscClass.mergingTbl(obj_oprClass.operation(obj_oprClass), clean_dict["variableName"].lower(), hc)
                del obj_oprClass, tbl_filtered, tot_overall, dist_p, temp_tbl, pivoted_tbl, final_tbl, prod_name
                gc.collect()
            else:
                tbl_filtered, dist_p = obj_oprClass.operation(obj_oprClass, flg4Daywise)
                prod_name = items.rsplit("_",1)[1]
                
                print "\nDay-wise Conversion breakup Table Stored for {0} as {1}".format(prod_name, prefix+"_conv_daywise_"+prod_name)
                tbl_filtered.write.mode("overwrite").saveAsTable(prefix+"_conv_daywise_"+prod_name)
                del obj_oprClass, tbl_filtered, dist_p, prod_name
                gc.collect()
        
        gc.collect()
            
        if flg4Analyzer == 'yes':
            query = "select * from {0}".format(prefix+'_Exp')
            tbl_exp = hc.sql(query)
            del query
            query = "select * from {0}".format(prefix+'_Trans')
            tbl_trans = hc.sql(query).withColumnRenamed('household_id', 'hhid')
            
            print "\n------------------------Generating Analyzer Tables------------------------\n"
            
            generateAnalyzerTables(tbl_exp, tbl_trans, prefix)
        else:
            print "User opted to not generate analyzer tables"
        
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