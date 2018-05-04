# -*- coding: utf-8 -*-
"""
Created on Mon May 29 16:19:10 2017
Module - Decile and Percentile
Description - This script is to generate TotalMarket deciles, WMSpend deciles, WMShare decile alongwith percentiles, and decile summaries.
@author: vn0bz25
"""
from pyspark import SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
from random import randint
import gc
import sys

class deciling_percentile:
    def add_to_dict(self,_dict, key, value):
        _dict[key] = value
        return _dict
    
    def get_percentile(self, x, strColName, total_num_rows):
        _dict, row_number = x
        percentile = x[1] / float(total_num_rows)
        return self.add_to_dict(_dict, strColName, percentile)
            
    
    def Deciling_Percentile(self,hc, tbl_scoring, bolPercentileFlg):
        tbl_scoring.withColumn("partitionCol",func.lit(randint(1,200))).registerTempTable('decile_temp_table')
        query = "select *, ntile(10) OVER (PARTITION BY partitionCol ORDER By WMSpendScore) AS WMSpendDecile, \
        ntile(10) OVER (PARTITION BY partitionCol ORDER By TotalMarketSpendScore) AS TMDecile,\
        ntile(10) OVER (PARTITION BY partitionCol ORDER By WMShareScore) AS WMShareDecile from decile_temp_table"
        ss = hc.sql(query)
        if(bolPercentileFlg):
            ss.registerTempTable('decile_table')
            total_num_rows = hc.sql('SELECT household_id FROM decile_table').count()
            gc.collect()
            rdd_percentile = hc.sql('SELECT * FROM decile_table').rdd.map(lambda x: x.asDict())\
            .map(lambda d: (d['TotalMarketSpendScore'], d)).sortByKey(ascending=False).map(lambda x: x[1]).zipWithIndex().map(lambda x: self.get_percentile(x,'TMSpendPercentile', total_num_rows))\
            .map(lambda d: (d['WMSpendScore'], d)).sortByKey(ascending=False).map(lambda x: x[1]).zipWithIndex().map(lambda x: self.get_percentile(x,'WMSpendPercentile', total_num_rows))\
            .map(lambda d: (d['WMShareScore'], d)).sortByKey(ascending=False).map(lambda x: x[1]).zipWithIndex().map(lambda x: self.get_percentile(x,'WMSharePercentile', total_num_rows))
            hc.createDataFrame(rdd_percentile).registerTempTable('final_scoring_temp_table')
            hc.dropTempTable('decile_table')
        else:
            ss.registerTempTable('final_scoring_temp_table')
            del tbl_scoring
            gc.collect()
            hc.dropTempTable('decile_temp_table')
        del ss, query
        gc.collect()
        
    def csvTohive(self, dfFile, strPrint, tblName):
        dfFile.fillna(0).write.mode('overwrite').saveAsTable(tblName,format='orc')
        print "\n%s created as %s"%(strPrint, tblName)
        
    def summary(self,hc,campaigntblname):
        print('Generating decile summary reports, look for %s_summary tables in CKPIRI\n'%campaigntblname)
        tbl = hc.sql('select wmsharescore, totalmarketspendscore, wmspendscore, wmspenddecile, tmdecile, wmsharedecile  from mars_candyDeciles_Oct17').toPandas()
        l_deciles = ('wmspenddecile', 'tmdecile', 'wmsharedecile')
        for i in range(3):
            groupeddf = tbl.groupby(l_deciles[i])
            gg = groupeddf.describe()[['totalmarketspendscore','wmsharescore','wmspendscore']]
            self.csvTohive(hc.createDataFrame(gg.reset_index()), 'Summary Generated as ', campaigntblname+'_'+str(i+1)+'_summary')
#            gg.to_excel(campaigntblname+'_'+str(i+1)+'_summary.xlsx',l_deciles[i]+'_summary')
            del gg
            gc.collect()
        del tbl, l_deciles
        gc.collect()
        
        
def main(argv):
    sc = SparkContext()    
    hc = HiveContext(sc)
    ##"""Referencing required Item and Cust_scan table"""
    hc.sql('USE CKPIRI')
#    with open('cust_scan_Pull.sql') as fp: # this query will reference cust_scan table, and pull distinct HH records for scoring.
    query = "select * from {}".format(argv[1])
    tbl_scoring = hc.sql(query)
    objDeciling_percentile = deciling_percentile()
    if len(argv) <= 2:
        flg4percentile = False
    else:
        flg4percentile = argv[2]
#    objDeciling_percentile.Deciling_Percentile(hc, tbl_scoring, bool(int(flg4percentile)))
    objDeciling_percentile.summary(hc,argv[1])
    
    finalTbl = hc.sql('SELECT * FROM final_scoring_temp_table').fillna(0)
    print("Merging all columns and loading table %s in CKPIRI..."%(argv[1]+'_withDeciles'))
    finalTbl.select([x for x in finalTbl.columns if 'partitionCol' not in x]).write.mode("overwrite").saveAsTable(argv[1]+'_withDeciles',format = "orc")
    print("%s table created, code terminating successfully !"%(argv[1]+'_withDeciles'))
    hc.dropTempTable('final_scoring_temp_table')

    sc.stop()

if __name__ == "__main__":
    main(sys.argv)