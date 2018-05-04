# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:45:45 2017
Module - Scoring Functions
Description - This module is supporting script for Scoring Module. It contains classes for unique operations required to generate variables for Scoring Households.
@author: vn0bz25
"""
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pyspark.sql.types as typ
import MappingScript as ms
import pandas as pd
import ast

class dynamicObjectCreation:
    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)
    
class totqty(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
        
    def func_totqtycatg(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .withColumn(str(self.dynProp.variableName), col("sum(unit_qty)")*self.dynProp.coefficient)
                
    def func_totqtydept(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .withColumn(str(self.dynProp.variableName), col("sum(unit_qty)")*self.dynProp.coefficient)
    
    def func_totqtyupc(self):
        pass
        
    func = dict(
        dept = func_totqtydept,
        catg = func_totqtycatg, 
        upc = func_totqtyupc
    )
    
class avgsaleitem(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_avgsaleitemcatg(self):
        Totsaleitem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','retail_price','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count','retail_price':'sum'})\
        .withColumnRenamed("sum(retail_price)",'Total_Sales')\
        .withColumnRenamed("count(unit_qty)",'Total_items')
        ## Average Sales per Item -- 0 when Total_items =0 else sale/items
        
        Avg_Sales_Item = Totsaleitem.withColumn('fnlColumn',\
        when(Totsaleitem.Total_items>0,Totsaleitem.Total_Sales/Totsaleitem.Total_items).otherwise(0))\
        .withColumn(str(self.dynProp.variableName), when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
        return Avg_Sales_Item
        
    def func_avgsaleitemdept(self):
        Totsaleitem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','retail_price','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count','retail_price':'sum'})\
        .withColumnRenamed("sum(retail_price)",'Total_Sales')\
        .withColumnRenamed("count(unit_qty)",'Total_items')
        ## Average Sales per Item -- 0 when Total_items =0 else sale/items
        
        Avg_Sales_Item = Totsaleitem.withColumn('fnlColumn',\
        when(Totsaleitem.Total_items>0,Totsaleitem.Total_Sales/Totsaleitem.Total_items).otherwise(0))\
        .withColumn(str(self.dynProp.variableName), when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        return Avg_Sales_Item
        
    func = dict(
        dept = func_avgsaleitemdept,
        catg = func_avgsaleitemcatg
    )
    
class avgsaletx(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_avgsaletxdept(self):
        avgsaletx_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','retail_price','store_nbr','visit_nbr')\
        .groupBy('household_id').agg((countDistinct(concat_ws('_','store_nbr','visit_nbr')).alias('Trip')),sum('retail_price').alias('Sales'))
        return avgsaletx_old.withColumn('fnlColumn',when(avgsaletx_old.Trip>0,avgsaletx_old.Sales/avgsaletx_old.Trip).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
    def func_avgsaletxcatg(self):
        avgsaletx_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','retail_price','store_nbr','visit_nbr',(concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg((countDistinct(concat_ws('_','store_nbr','visit_nbr')).alias('Trip')),sum('retail_price').alias('Sales'))
        return avgsaletx_old.withColumn('fnlColumn',when(avgsaletx_old.Trip>0,avgsaletx_old.Sales/avgsaletx_old.Trip).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
    func = dict(
        dept = func_avgsaletxdept,
        catg = func_avgsaletxcatg
    )
    
class hasshop(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_hasshopdept(self):
        has_shop_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','retail_price') \
        .groupBy('household_id').sum('retail_price') \
        .withColumnRenamed("sum(retail_price)",'Sales')
        
        has_shop_new = has_shop_old.withColumn('fnlColumn',when(has_shop_old.Sales>0,1).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),col('fnlColumn')*self.dynProp.coefficient)
        return has_shop_new
        
    def func_hasshopcatg(self):
        has_shop_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','retail_price') \
        .groupBy('household_id').sum('retail_price') \
        .withColumnRenamed("sum(retail_price)",'Sales')
        
        has_shop_new = has_shop_old.withColumn('fnlColumn',when(has_shop_old.Sales>0,1).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),col('fnlColumn')*self.dynProp.coefficient)
        return has_shop_new
        
    func = dict(
        dept = func_hasshopdept,
        catg = func_hasshopcatg
    )
    
class mosales(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_mosalesdept(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','visit_date')\
        .groupBy('household_id').agg(countDistinct(month('visit_date')).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
    def func_mosalescatg(self):
        print("In MO Category")
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','visit_date')\
        .groupBy('household_id').agg(countDistinct(month('visit_date')).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
    func = dict(
        dept = func_mosalesdept,
        catg = func_mosalescatg
    )
    
#class pctsales(dynamicObjectCreation):
#    def __init__(self, tScan, strOprKey, clean_dict):
#        self.tblScan = tScan
#        self.operation = self.func[strOprKey]
#        self.dynProp = dynamicObjectCreation(clean_dict)
#                
#    def func_pctsalesdept(self):
#        pass
#    def func_pctsalescatg(self):
#        pass
#    func = dict(
#        dept = func_pctsalesdept,
#        catg = func_pctsalescatg
#    )

class sale(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                        
    def func_saledept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        if not bool(self.dynProp.wmspend):
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale            
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)\
            .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
            
    def func_salecatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        if not bool(self.dynProp.wmspend):
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_category_nbr.isin(cat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_category_nbr.isin(cat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)\
            .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
        
    def func_salesubcatg(self):
        if type(self.dynProp.subcategory) == unicode:
            subcat_list = list(ast.literal_eval(self.dynProp.subcategory))
        else:
            subcat_list = self.dynProp.subcategory
        if not bool(self.dynProp.wmspend):
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_subcatg_nbr.isin(subcat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_subcatg_nbr.isin(subcat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)\
            .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
            
    def func_saleupc(self):
        if not bool(self.dynProp.wmspend):
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale
        else:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)\
                .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
            
    def func_salefineline(self):
        if type(self.dynProp.fineline) == unicode:
            fineline_list = list(ast.literal_eval(self.dynProp.fineline))
        else:
            fineline_list = self.dynProp.fineline
        if not bool(self.dynProp.wmspend):
            print "Inside IF and {0}".format(fineline_list)
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.fineline_nbr.isin(fineline_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale
        else:
            print "Inside ELSE and {0}".format(fineline_list)
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.fineline_nbr.isin(fineline_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)")*self.dynProp.coefficient)\
            .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale


    func = dict(
        dept = func_saledept,
        catg = func_salecatg,
        subcatg = func_salesubcatg,
        upc = func_saleupc,
        fineline = func_salefineline
    )
   
#class sale(dynamicObjectCreation):
#    def __init__(self, tScan, strOprKey, clean_dict):
#        self.tblScan = tScan
#        self.operation = self.func[strOprKey]
#        self.dynProp = dynamicObjectCreation(clean_dict)
#                        
#    def func_saledept(self):
#        if not bool(self.dynProp.wmspend):
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)
#            return totSale            
#        else:
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)\
#            .withColumnRenamed("sum(retail_price)",'Total_Sales')
#            return totSale
#            
#    def func_salecatg(self):
#        if not bool(self.dynProp.wmspend):
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)
#            return totSale
#        else:
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)\
#            .withColumnRenamed("sum(retail_price)",'Total_Sales')
#            return totSale
#            
#    def func_salesubcatg(self):
#        if not bool(self.dynProp.wmspend):
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.dept_subcatg_nbr == self.dynProp.subcategory)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)
#            return totSale
#        else:
#            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#            .filter(self.tblScan.dept_subcatg_nbr == self.dynProp.subcategory)\
#            .select('household_id','retail_price') \
#            .groupBy('household_id').agg({'retail_price':'sum'})\
#            .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)\
#            .withColumnRenamed("sum(retail_price)",'Total_Sales')
#            return totSale
#
#    func = dict(
#        dept = func_saledept,
#        catg = func_salecatg,
#        subcatg = func_salesubcatg
#    )
    
class totitem(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_totitemdept(self):
        totItem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count'})\
        .withColumn(str(self.dynProp.variableName),when(col("count(unit_qty)")>0,col("count(unit_qty)")*self.dynProp.coefficient).otherwise(0))
        return totItem
        
    def func_totitemcatg(self):
        totItem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count'})\
        .withColumn(str(self.dynProp.variableName),when(col("count(unit_qty)")>0,col("count(unit_qty)")*self.dynProp.coefficient).otherwise(0))
        return totItem
        
    func = dict(
        dept = func_totitemdept,
        catg = func_totitemcatg
    )
    
class tottx(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_tottxdept(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','store_nbr','visit_nbr',(concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg(countDistinct(concat_ws('_','store_nbr','visit_nbr').alias('trip') ).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
 
    def func_tottxcatg(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','store_nbr','visit_nbr',(concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg(countDistinct(concat_ws('_','store_nbr','visit_nbr').alias('trip') ).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))

    func = dict(
        dept = func_tottxdept,
        catg = func_tottxcatg
    )
    
#class wkslsttx(dynamicObjectCreation):
#    def __init__(self, tScan, strOprKey, clean_dict):
#        self.tblScan = tScan
#        self.operation = self.func[strOprKey]
#        self.dynProp = dynamicObjectCreation(clean_dict)
#                
#    def func_wkslsttxdept(self):
#        if type(self.dynProp.department) == unicode:
#            dept_list = list(ast.literal_eval(self.dynProp.department))
#        else:
#            dept_list = self.dynProp.department
#        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
#        .withColumn('weeks',f.datediff(f.to_date(f.lit(str(self.dynProp.enddate.date()))), self.tblScan.visit_date)/7)\
#        .select(self.tblScan.household_id,self.tblScan.visit_date,f.col('weeks')).groupBy(self.tblScan.household_id).agg(f.min(f.col('weeks')))\
#        .withColumnRenamed("min(weeks)",str(self.dynProp.variableName))
#        
#    def func_wkslsttxcatg(self):
#        if type(self.dynProp.category) == unicode:
#            cat_list = list(ast.literal_eval(self.dynProp.category))
#        else:
#            cat_list = self.dynProp.category
#        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
#        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
#        .withColumn('weeks',f.datediff(f.to_date(f.lit(str(self.dynProp.enddate.date()))), self.tblScan.visit_date)/7)\
#        .select(self.tblScan.household_id,self.tblScan.visit_date,f.col('weeks')).groupBy(self.tblScan.household_id).agg(f.min(f.col('weeks')))\
#        .withColumnRenamed("min(weeks)",str(self.dynProp.variableName))
#        
#    func = dict(
#        dept = func_wkslsttxdept,
#        catg = func_wkslsttxcatg
#        )
        
class unqitem(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
        
    def func_uniqitemdept(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.acctg_dept_nbr == self.dynProp.department)\
        .select('household_id','scan_id') \
        .groupBy('household_id').agg(countDistinct('scan_id').alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
        
    def func_uniqitemcatg(self):
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .filter(self.tblScan.dept_category_nbr == self.dynProp.category)\
        .select('household_id','scan_id') \
        .groupBy('household_id').agg(countDistinct('scan_id').alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),when(col('fnlColumn')>0,col('fnlColumn')*self.dynProp.coefficient).otherwise(0))
     
    func = dict(
        dept = func_uniqitemdept,
        catg = func_uniqitemcatg
        )       
        
class demo(dynamicObjectCreation):
    def __init__(self, tdemo, dfmappingSheet, strOprKey, col_name, clean_dict):
        self.tbldemo = tdemo
        self.mappingsheet = dfmappingSheet
        self.col_name = col_name
        self.dynProp = dynamicObjectCreation(clean_dict)
        self.operation = self.func[strOprKey]
    
    def funcDemoCreation(self):
        return self.tbldemo\
        .join(self.mappingsheet, col(self.col_name)==col('var_value'), 'left').withColumn(str(self.dynProp.variableName),col('Value')*self.dynProp.coefficient)\
        .select(col('household_id'),col(self.dynProp.variableName))
        
    def funcAgeEstVar(self):
        return self.tbldemo\
        .select('household_id',col('hhh_age').alias(str(self.dynProp.variableName)))
        
    def funcRangeforIncome(self):
        t = self.tbldemo\
        .join(self.mappingsheet, col(self.col_name)==col('var_value'), 'left').select(col('household_id'),col('Value'))
        return {
            'is_income_over_50k' : self.funcIncomeGreater50k,
            'is_income_over_100k' : self.funcIncomeGreater100000k,
            'is_income_40k_65k' : self.funcIncome40kand65k
        }[str(self.dynProp.variableName).lower()](t)
        
    def funcIncomeGreater50k(self, tbl):
        return tbl.withColumn(self.dynProp.variableName, when(col('Value') > 50000, 1*self.dynProp.coefficient).otherwise(0)) 
    
    def funcIncomeGreater100000k(self, tbl):
        return tbl.withColumn(self.dynProp.variableName, when(col('Value') > 100000, 1*self.dynProp.coefficient).otherwise(0))
    
    def funcIncome40kand65k(self, tbl):
        return tbl.withColumn(self.dynProp.variableName, when((col('Value') > 40000) & (col('Value') < 65000), 1*self.dynProp.coefficient).otherwise(0)) 
       
    func = dict(
        demo = funcDemoCreation,
        age = funcAgeEstVar,
        rangeincome = funcRangeforIncome
        )
        
class custom(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
    
    def func_saleupc(self):
        if not bool(self.dynProp.wmspend):
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',when(col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)
            return totSale
        else:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',when(col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),col("sum(retail_price)")*self.dynProp.coefficient)\
                .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
    func = dict(
        upc = func_saleupc
        )