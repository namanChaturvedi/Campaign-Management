# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 12:45:45 2017

@author: vn0bz25
"""
import pyspark.sql.functions as f
import pyspark.sql.types as typ
import MappingScript as ms
import ast

#class funcMapping:
#    def dept(self): return self.func['dept']
#    def catg(self): return self.func['catg']
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
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .withColumn(str(self.dynProp.variableName), f.col("sum(unit_qty)"))
                
    def func_totqtydept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .withColumn(str(self.dynProp.variableName), f.col("sum(unit_qty)"))
        
    def func_totqtyupc(self):
        if type(self.dynProp.upc) == unicode:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
        else:
            upc_list = self.dynProp.upc
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.upc_nbr.isin(upc_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'sum'})\
        .withColumn(str(self.dynProp.variableName), f.col("sum(unit_qty)"))
        
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
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        Totsaleitem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','retail_price','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count','retail_price':'sum'})\
        .withColumnRenamed("sum(retail_price)",'Total_Sales')\
        .withColumnRenamed("count(unit_qty)",'Total_items')
        ## Average Sales per Item -- 0 when Total_items =0 else sale/items
        
        Avg_Sales_Item = Totsaleitem.withColumn('fnlColumn',\
        f.when(Totsaleitem.Total_items>0,Totsaleitem.Total_Sales/Totsaleitem.Total_items).otherwise(0))\
        .withColumn(str(self.dynProp.variableName), f.col('fnlColumn'))
        
        return Avg_Sales_Item
        
    def func_avgsaleitemdept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        Totsaleitem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','retail_price','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count','retail_price':'sum'})\
        .withColumnRenamed("sum(retail_price)",'Total_Sales')\
        .withColumnRenamed("count(unit_qty)",'Total_items')
        ## Average Sales per Item -- 0 when Total_items =0 else sale/items
        
        Avg_Sales_Item = Totsaleitem.withColumn('fnlColumn',\
        f.when(Totsaleitem.Total_items>0,Totsaleitem.Total_Sales/Totsaleitem.Total_items).otherwise(0))\
        .withColumn(str(self.dynProp.variableName), f.col('fnlColumn'))
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
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        avgsaletx_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','retail_price','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg((f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr')).alias('Trip')),f.sum('retail_price').alias('Sales'))
        return avgsaletx_old.withColumn('fnlColumn',f.when(avgsaletx_old.Trip>0,avgsaletx_old.Sales/avgsaletx_old.Trip).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    def func_avgsaletxcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        avgsaletx_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','retail_price','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg((f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr')).alias('Trip')),f.sum('retail_price').alias('Sales'))
        return avgsaletx_old.withColumn('fnlColumn',f.when(avgsaletx_old.Trip>0,avgsaletx_old.Sales/avgsaletx_old.Trip).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
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
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        has_shop_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','retail_price') \
        .groupBy('household_id').sum('retail_price') \
        .withColumnRenamed("sum(retail_price)",'Sales')
        
        has_shop_new = has_shop_old.withColumn('fnlColumn',f.when(has_shop_old.Sales>0,1).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        return has_shop_new
        
    def func_hasshopcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        has_shop_old= self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','retail_price') \
        .groupBy('household_id').sum('retail_price') \
        .withColumnRenamed("sum(retail_price)",'Sales')
        
        has_shop_new = has_shop_old.withColumn('fnlColumn',f.when(has_shop_old.Sales>0,1).otherwise(0))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
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
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','visit_date')\
        .groupBy('household_id').agg(f.countDistinct(f.month('visit_date')).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    def func_mosalescatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','visit_date')\
        .groupBy('household_id').agg(f.countDistinct(f.month('visit_date')).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    def func_mosalesupc(self):
        upc_list = list(ast.literal_eval(self.dynProp.upc))        
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.upc_nbr.isin(upc_list))\
        .select('household_id','visit_date')\
        .groupBy('household_id').agg(f.countDistinct(f.month('visit_date')).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    func = dict(
        dept = func_mosalesdept,
        catg = func_mosalescatg,
        upc = func_mosalesupc
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
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale            
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
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
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_category_nbr.isin(cat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
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
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.dept_subcatg_nbr.isin(subcat_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
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
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale
        else:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
                .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
            
    def func_salefineline(self):
        if type(self.dynProp.fineline) == unicode:
            fineline_list = list(ast.literal_eval(self.dynProp.fineline))
        else:
            fineline_list = self.dynProp.fineline
        if not bool(self.dynProp.wmspend):
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.fineline_nbr.isin(fineline_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale
        else:
            totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .where(self.tblScan.fineline_nbr.isin(fineline_list))\
            .select('household_id','retail_price') \
            .groupBy('household_id').agg({'retail_price':'sum'})\
            .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
            .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale

    func = dict(
        dept = func_saledept,
        catg = func_salecatg,
        subcatg = func_salesubcatg,
        upc = func_saleupc,
        fineline = func_salefineline
    )
    
    
class totitem(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_totitemdept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        totItem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count'})\
        .withColumn(str(self.dynProp.variableName),f.col("count(unit_qty)"))
        return totItem
        
    def func_totitemcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        totItem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count'})\
        .withColumn(str(self.dynProp.variableName),f.col("count(unit_qty)"))
        return totItem
    
    def func_totitemsubcatg(self):
        if type(self.dynProp.subcategory) == unicode:
            subcat_list = list(ast.literal_eval(self.dynProp.subcategory))
        else:
            subcat_list = self.dynProp.subcategory
        totItem = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_subcatg_nbr.isin(subcat_list))\
        .select('household_id','unit_qty') \
        .groupBy('household_id').agg({'unit_qty':'count'})\
        .withColumn(str(self.dynProp.variableName),f.col("count(unit_qty)"))
        return totItem
        
    func = dict(
        dept = func_totitemdept,
        catg = func_totitemcatg,
        subcatg = func_totitemsubcatg
    )
    
class tottx(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_tottxdept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg(f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip') ).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
 
    def func_tottxcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg(f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip') ).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    def func_tottxupc(self):
        if type(self.dynProp.upc) == unicode:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
        else:
            upc_list = self.dynProp.upc
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.upc_nbr.isin(upc_list))\
        .select('household_id','store_nbr','visit_nbr',(f.concat_ws('_','store_nbr','visit_nbr').alias('tran_key') ))\
        .groupBy('household_id').agg(f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip') ).alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))

    func = dict(
        dept = func_tottxdept,
        catg = func_tottxcatg,
        upc = func_tottxupc
    )
    
class wkslsttx(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                
    def func_wkslsttxdept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .withColumn('weeks',f.datediff(f.to_date(f.lit(str(self.dynProp.enddate.date()))), self.tblScan.visit_date)/7)\
        .select(self.tblScan.household_id,self.tblScan.visit_date,f.col('weeks')).groupBy(self.tblScan.household_id).agg(f.min(f.col('weeks')))\
        .withColumnRenamed("min(weeks)",str(self.dynProp.variableName))
        
    def func_wkslsttxcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .withColumn('weeks',f.datediff(f.to_date(f.lit(str(self.dynProp.enddate.date()))), self.tblScan.visit_date)/7)\
        .select(self.tblScan.household_id,self.tblScan.visit_date,f.col('weeks')).groupBy(self.tblScan.household_id).agg(f.min(f.col('weeks')))\
        .withColumnRenamed("min(weeks)",str(self.dynProp.variableName))
        
    def func_wkslsttxupc(self):
        upc_list = list(ast.literal_eval(self.dynProp.upc))
            
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.upc_nbr.isin(upc_list))\
        .withColumn('weeks',f.datediff(f.to_date(f.lit(str(self.dynProp.enddate.date()))), self.tblScan.visit_date)/7)\
        .select(self.tblScan.household_id,self.tblScan.visit_date,f.col('weeks')).groupBy(self.tblScan.household_id).agg(f.min(f.col('weeks')))\
        .withColumnRenamed("min(weeks)",str(self.dynProp.variableName))
        
    func = dict(
        dept = func_wkslsttxdept,
        catg = func_wkslsttxcatg, 
        upc = func_wkslsttxupc
        )
        
class unqitem(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
        
    def func_uniqitemdept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','scan_id') \
        .groupBy('household_id').agg(f.countDistinct('scan_id').alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
        
    def func_uniqitemcatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        return self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','scan_id') \
        .groupBy('household_id').agg(f.countDistinct('scan_id').alias('fnlColumn'))\
        .withColumn(str(self.dynProp.variableName),f.col('fnlColumn'))
     
    func = dict(
        dept = func_uniqitemdept,
        catg = func_uniqitemcatg
        )       

class pctsale(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
                        
    def func_pctsaledept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        
        totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .withColumn('dept_present',f.when(self.tblScan.acctg_dept_nbr.isin(dept_list),1).otherwise(0))\
        .select('household_id','retail_price','dept_present')\
        .groupBy('household_id', 'dept_present').agg({'retail_price':'sum'})
        
        pctSale = totSale.withColumn('sale', f.when(totSale.dept_present == 1, f.col('sum(retail_price)')).otherwise(0))\
        .withColumnRenamed('sum(retail_price)', 'o_sale')
        
        fnlSale = pctSale.groupBy('household_id').sum('sale', 'o_sale')\
        .withColumn(str(self.dynProp.variableName),f.col('sum(sale)')/f.col('sum(o_sale)'))
        return fnlSale          
            
    def func_pctsalecatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .withColumn('cat_present',f.when(self.tblScan.dept_category_nbr.isin(cat_list),1).otherwise(0))\
        .select('household_id','retail_price','cat_present')\
        .groupBy('household_id', 'cat_present').agg({'retail_price':'sum'})
        
        pctSale = totSale.withColumn('sale', f.when(totSale.cat_present == 1, f.col('sum(retail_price)')).otherwise(0))\
        .withColumnRenamed('sum(retail_price)', 'o_sale')
        
        fnlSale = pctSale.groupBy('household_id').sum('sale', 'o_sale')\
        .withColumn(str(self.dynProp.variableName),f.col('sum(sale)')/f.col('sum(o_sale)'))
        return fnlSale          
        
        
    def func_pctsalesubcatg(self):
        if type(self.dynProp.subcategory) == unicode:
            subcat_list = list(ast.literal_eval(self.dynProp.subcategory))
        else:
            subcat_list = self.dynProp.subcategory

        totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
        .withColumn('subcat_present',f.when(self.tblScan.dept_subcatg_nbr.isin(subcat_list),1).otherwise(0))\
        .select('household_id','retail_price','subcat_present')\
        .groupBy('household_id', 'subcat_present').agg({'retail_price':'sum'})
        
        pctSale = totSale.withColumn('sale', f.when(totSale.subcat_present == 1, f.col('sum(retail_price)')).otherwise(0))\
        .withColumnRenamed('sum(retail_price)', 'o_sale')
        
        fnlSale = pctSale.groupBy('household_id').sum('sale', 'o_sale')\
        .withColumn(str(self.dynProp.variableName),f.col('sum(sale)')/f.col('sum(o_sale)'))
        return fnlSale 
            
    def func_pctsaleupc(self):   
        upc_list = list(ast.literal_eval(self.dynProp.upc))
        totSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
            .select('household_id', 'retail_price', 'upc_nbr')\
            .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))\
            .groupBy('household_id', 'upc_present').agg({'retail_price':'sum'})
            
        pctSale = totSale.withColumn('sale', f.when(totSale.upc_present == 1, f.col('sum(retail_price)')).otherwise(0))\
        .withColumnRenamed('sum(retail_price)', 'o_sale')
        
        fnlSale = pctSale.groupBy('household_id').sum('sale', 'o_sale')\
        .withColumn(str(self.dynProp.variableName),f.col('sum(sale)')/f.col('sum(o_sale)'))
        return fnlSale 
        
    func = dict(
        dept = func_pctsaledept,
        catg = func_pctsalecatg,
        subcatg = func_pctsalesubcatg,
        upc = func_pctsaleupc
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
        .join(self.mappingsheet, f.col(self.col_name)==f.col('var_value'), 'left').withColumn(str(self.dynProp.variableName),f.col('Value'))\
        .select(f.col('household_id'),f.col(self.dynProp.variableName))
        
    def funcAgeEstVar(self):
        return self.tbldemo\
        .select('household_id',f.col('hhh_age').alias(str(self.dynProp.variableName)))
        
    def funcRangeforIncome(self):
        t = self.tbldemo\
        .join(self.mappingsheet, f.col(self.col_name)==f.col('var_value'), 'left').select(f.col('household_id'),f.col('Value'))
        return {
            'is_income_over_50k' : self.funcIncomeGreater50k,
            'is_income_over_100k' : self.funcIncomeGreater100000k,
            'is_income_40k_65k' : self.funcIncome40kand65k
        }[str(self.dynProp.variableName).lower()](t)
        
    def funcIncomeGreater50k(self, tbl):
        return tbl.withColumn(str(self.dynProp.variableName), f.when(f.col('Value') > 50000, 1).otherwise(0))
    
    def funcIncomeGreater100000k(self, tbl):
        return tbl.withColumn(str(self.dynProp.variableName), f.when(f.col('Value') > 100000, 1).otherwise(0))
    
    def funcIncome40kand65k(self, tbl):
        return tbl.withColumn(str(self.dynProp.variableName), f.when((f.col('Value') > 40000) & (f.col('Value') < 65000), 1).otherwise(0))
       
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
                .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))
            return totSale
        else:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
            filterSale = self.tblScan.filter(self.tblScan.visit_date >= str(ms.periodCalc(self.dynProp.enddate.strftime('%Y-%m-%d'), int(self.dynProp.timeperiod)))) \
                .select('household_id', 'retail_price', 'upc_nbr')\
                .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
            totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price') \
                .groupBy('household_id').agg({'retail_price':'sum'})\
                .withColumn(str(self.dynProp.variableName),f.col("sum(retail_price)"))\
                .withColumnRenamed("sum(retail_price)",'Total_Sales')
            return totSale
    func = dict(
        upc = func_saleupc,
        )