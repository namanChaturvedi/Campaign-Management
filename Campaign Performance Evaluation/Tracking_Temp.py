# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:59:08 2017
Module - Tracking Automation
Description - This script automate the process of Tracking campaigns
@author: vn0bz25
"""
#Importing required Libraries
from __future__ import division
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import scipy.stats as stats
from scipy.special import stdtr
import pyspark.sql.types as typ
import pyspark.sql.functions as f
import pandas as pd
import MappingScript as ms
import numpy as np
import traceback
import sys
import ast
import gc

#This class is for validating Input Sheet
class codeValidation:
    def __init__(self):
        print("INPUT SHEET VALIDATION IN PROGRESS TO CHECK FOR VALID INPUT VALUES")
        
    def isNaN(self,num):
        return num !=num
    
    def functoValidate(self, inputSheet):
        if not all(inputSheet['operation'].str.lower().isin(['flag','yes','no','both']).tolist()):
            print("Pass valid operation i.e. flag")
            return False
        else:
            print("INPUT SHEET VALIDATION SUCCESSFULL !!")
            return True

#This class is to identify variables, segments when generating Segment and Overall reports
class createFilterObj4Keys:
    def __init__(self, uniquekey, segments, strMethod):
        if strMethod == "segment":
            dicSegments = {}
            for item in segments:
                dicSegments[item.upper()] = item
            self.seg_var = dicSegments[uniquekey.split('SPEND')[1]]+'_segment'
        elif strMethod == "weighted":
            dicSegments = {}
            for item in segments:
                dicSegments[item.upper()] = item
            self.seg_var = dicSegments[uniquekey.split('SPEND')[1]]+'_segment'
            self.ratio_var = uniquekey.split('SPEND')[1].lower() + '_ratio'
        else:
            self.seg_var = 'Overall'
        self.prod_var = 'tot_spend_' + uniquekey.split('SPEND')[0].lower()
        self.response_var = 'buyer_' + uniquekey.split('SPEND')[0].lower()
        self.trip_var = 'trip_' + uniquekey.split('SPEND')[0].lower()
        
#This class is to identify variables, segments when generating Segment and Overall reports
class createFilterObj4Val:
    def __init__(self, uniquekey, columns, strMethod):
        _split_by_spend = uniquekey.split('SPEND')
        _split_by_buyer = _split_by_spend[1].split('CBUYER')
        if (strMethod == "segment") | (strMethod == "weighted"):
            self.subseg_var = self.identifySegVar([y for y in columns if 'segment' in y and y[-2:] in ('_t','_c')],_split_by_buyer[0].lower(),_split_by_buyer[1][0])
        else:
            self.subseg_var = 'Overall'
        self.holdout_flg = _split_by_buyer[1][0]
        self.response_flg = int(_split_by_buyer[1][1])
                
    def identifySegVar(self, segments, strFrmKey, flg_M_H):
        dicSegment = {}
        for item in segments:
            dicSegment[item.lower()] = item
        return {
            'M' : dicSegment[strFrmKey+'segment_t'],
            'H' : dicSegment[strFrmKey+'segment_c']
        }[flg_M_H]

#This is main Class with functions relevant to important tracking Process
class ExperianMerge(createFilterObj4Keys, createFilterObj4Val):
    def __init__(self, expName, scnName, end, start):
        self.expTblName = expName
        self.scanTbl = scnName
        self.startdate = start
        self.enddate = end
        
    def testCtrlMerge(self, hc):
        query = "select * from {}".format(self.expTblName)
        Maictrl_table=hc.sql(query).toPandas()
        Maictrl_table.columns=[x.title() for x in Maictrl_table.columns]
        dfm = pd.read_fwf("Mail.txt", header=None,names=['Household_Id','Mail_Holdout','Campaign_Id','Version_Id','Campaign_Cell_Id'], widths = [12,1,30,20,35])
        dfc = pd.read_fwf("Holdout.txt", header=None,names=['Household_Id','Mail_Holdout','Campaign_Id','Version_Id','Campaign_Cell_Id'], widths = [12,1,30,20,35])
        exp=dfm.append(dfc)
        exp['Household_Id'].astype(int)
        Maictrl_table.drop(Maictrl_table[['Mail_Holdout','Version_Id','Campaign_Id','Campaign_Cell_Id']],axis=1,inplace=True)
        Exp_table=pd.merge(exp,Maictrl_table,on='Household_Id',how='inner')
        Testflags = [x[0:-2] for x in Exp_table.columns if (x.endswith('_T'))]
        return self.expmovement(Exp_table, Testflags)
    
    def expmovement(self, Datasets, flags):
        for flag in flags:
            MailFlags=flag+"_T"
            HoldFlags=flag+"_C"
            Datasets.loc[(Datasets['Mail_Holdout']=='H') & (Datasets[MailFlags]==1),[MailFlags,HoldFlags]] = pd.DataFrame([0,1]).T.values
            Datasets.loc[(Datasets['Mail_Holdout']=='M') & (Datasets[HoldFlags]==1),[HoldFlags,MailFlags]]=pd.DataFrame([0,1]).T.values                              
        return Datasets
        
    def scnMergeExp(self, hc, exp):
        query = "select * from {}".format(self.scanTbl)
        tbl_S = hc.sql(query)
        return tbl_S.filter(tbl_S.visit_date.between(self.startdate.strftime('%Y-%m-%d'),self.enddate.strftime('%Y-%m-%d')))\
        .join(exp, tbl_S.household_id == exp.hhid, 'left').select([x for x in tbl_S.schema.names])
           
    def mergingTbl(self, dfToJoin, colName, buyerflg, tripflg, hc):
        dfToJoin.registerTempTable('TempHHTable')
        query = "SELECT a.*, b.{0}, b.{1}, b.{2} from tracking_temp_table a Left join TempHHTable b on a.household_id = b.household_id".format(colName, buyerflg, tripflg)
        ss = hc.sql(query)
        ss.registerTempTable('tracking_temp_table')
        hc.dropTempTable('TempHHTable')
        del ss, query
        gc.collect()
        
    def csvTohive(self, dfFile, strPrint, tblName):
        dfFile.fillna(0).write.mode('overwrite').saveAsTable(tblName,format='orc')
        print "%s created as %s"%(strPrint, tblName)
        gc.collect()
                
    def testFlagTrackingReport(self, Dataset, lsProd, lsSegment):
        gc.collect()
        #Initializing lists to be converted to dataframe columns
        lsCat, lsCount, lsMean, lsStdDev, lsVar = [], [], [], [], []
        for div in lsProd:
            #print tot_spend
            for segment in lsSegment:
                #print segment
                col = "pur_" + segment
                Dataset1=Dataset.loc[Dataset[segment]==1]
                Dataset1[col] = Dataset1[segment] * Dataset1[div]
                lsCount.append((Dataset1[Dataset1[col]>0][col]).count())
                lsMean.append((Dataset1[Dataset1[col]>0][col]).mean())
                lsStdDev.append((Dataset1[col]).std())
                lsCat.append(div.split('_')[2])
                lsVar.append(col)
        Df = pd.DataFrame({"Test_Flag":lsVar,"Buyers":lsCount,"Spend":lsMean,"Std_Dev":lsStdDev,"File_name":lsCat})
        return Df[["Test_Flag","Buyers","Spend","Std_Dev","File_name"]]
        
    def testFlagHHCountReport(self, Dataset, lsSegment):
        lsCount, lsVar = [], []
        for segment in lsSegment:
            lsCount.append((Dataset[Dataset[segment]>0][segment]).count())
            lsVar.append(segment)
        Df = pd.DataFrame({"Test_Flag":lsVar,"HH_count":lsCount})
        return Df[["Test_Flag","HH_count"]]
        
    def adjustMailControlGrpSegment(self, Dataset, lsSegment, unq_seg, unq_subseg):
        lsCount, lsVar, lsSeg, lsSubseg, lsGrp = [], [], [], [], []
        for segment in lsSegment:
            lsSeg.append([x for x in unq_seg if x in segment][0])
            lsSubseg.append([x for x in unq_subseg if x in segment][0])
            lsCount.append((Dataset[Dataset[segment]>0][segment]).count())
            lsVar.append(segment)
            lsGrp.append(segment.split('_')[-1].lower())
        Df = pd.DataFrame({"Test_Flag":lsVar,"HH_count":lsCount, "Segment":lsSeg, "SubSegment":lsSubseg, "Test_Control":lsGrp})
        Df = Df.query("Segment != SubSegment")
        Df_pivoted = Df.pivot_table(index = ["Segment","SubSegment"],columns = "Test_Control", values = "HH_count")
        Df_pivoted.reset_index(inplace = True)
        Df_pivoted['Control_to_Mail'] = (Df_pivoted.c/Df_pivoted.t)*100
        Df_pivoted['min_per_seg'] = Df_pivoted.groupby(['Segment'])['Control_to_Mail'].transform(min)
        Df_pivoted['Adjusted_c'] = (Df_pivoted.min_per_seg/100)*Df_pivoted.t
        del lsSeg, lsSubseg, lsCount, lsVar, lsGrp
        test_adj = Df_pivoted[["SubSegment", "t"]]
        test_adj["SubSegment"] = test_adj["SubSegment"] + '_t'
        test_adj.rename(columns = {"t" : "FinalSize"}, inplace = True)
        #Creating Adjusted Sample Size Table           
        control_adj_1 = Df_pivoted[["SubSegment", "Adjusted_c"]]
        control_adj_1["SubSegment"], control_adj_1["Adjusted_c"] = control_adj_1["SubSegment"] + '_c', control_adj_1["Adjusted_c"].fillna(0).apply(lambda x : round(x))
        control_adj_1.rename(columns = {"Adjusted_c" : "FinalSize"}, inplace = True)
        final_sample_size_adj = pd.concat([control_adj_1,test_adj])
        
        control_adj_2 = Df_pivoted[["SubSegment", "c"]]
        control_adj_2["SubSegment"] = control_adj_2["SubSegment"] + '_c'
        control_adj_2.rename(columns = {"c" : "FinalSize"}, inplace = True)
        final_sample_size_nonadj = pd.concat([control_adj_2, test_adj])            
        return Df_pivoted, final_sample_size_adj, final_sample_size_nonadj            
            
    def adjustMailControlGrpOverall(self, Dataset, unq_seg, unq_subseg):
        lsCount, lsVar, lsSeg, lsGrp, lsCamp = [], [], [], [], []
        for segment in unq_subseg:
            lsSeg.append([x for x in unq_seg if x in segment][0])
            lsCount.append((Dataset[Dataset[segment]>0][segment]).count())
            lsVar.append(segment)
            lsGrp.append(segment.split('_')[-1].lower())
            lsCamp.append('_'.join(segment.split('_')[:1]))
        Df = pd.DataFrame({"Test_Flag":lsVar,"HH_count":lsCount, "Segment":lsSeg, "Test_Control":lsGrp, "Campaign" : lsCamp})
        Df_pivoted = Df.pivot_table(index = ["Segment", "Campaign"],columns = "Test_Control", values = "HH_count")
        Df_pivoted.reset_index(inplace = True)
        Df_pivoted['Control_to_Mail'] = (Df_pivoted.c/Df_pivoted.t)*100
        Df_pivoted['min_overall'] = Df_pivoted.groupby(["Campaign"])['Control_to_Mail'].transform(min)
        Df_pivoted['Adjusted_c'] = (Df_pivoted.min_overall/100)*Df_pivoted.t
        del lsSeg, lsCount, lsVar, lsGrp
        test_adj = Df_pivoted[["Segment", "t"]]
        test_adj["Segment"] = test_adj["Segment"] + '_t'
        test_adj.rename(columns = {"t" : "FinalSize"}, inplace = True)
        control_adj_1 = Df_pivoted[["Segment", "Adjusted_c"]]
        control_adj_1["Segment"], control_adj_1["Adjusted_c"] = control_adj_1["Segment"] + '_c', control_adj_1["Adjusted_c"].fillna(0).apply(lambda x : round(x))
        control_adj_1.rename(columns = {"Adjusted_c" : "FinalSize"}, inplace = True)
        final_sample_size = pd.concat([control_adj_1, test_adj])
        final_sample_size['SubSegment'] = final_sample_size.apply(lambda x : '_'.join(x.Segment.rsplit('_',1)[:1] + ['segment',] + x.Segment.rsplit('_',1)[1:]), axis = 1)
        return Df_pivoted, final_sample_size
            
    def creatingAdjustedExpTbl(self, Dataset, adjSample, unq_seg, strAdj, hc):
        strHH = str([x for x in Dataset.columns if x.lower() == 'household_id'][0]) #pulling out exact Household_id keyword in Experian Merged Table
        strMH = str([x for x in Dataset.columns if x.lower() == 'mail_holdout'][0]) #pulling out exact Mail_holdout keyword in Experian Merged Table
        print Dataset.columns
        final_adj_df = pd.DataFrame(columns = Dataset.columns)
        for index, row in adjSample.iterrows():
            temp_df = Dataset[Dataset[row.SubSegment] == 1][[strHH, strMH]] #.sample(n = row.FinalSize)
            temp_t = temp_df.set_index(np.random.permutation(temp_df.index))
            temp_df = temp_t[:int(row.FinalSize)]
            temp_df[row.SubSegment] = 1
            final_adj_df = pd.concat([final_adj_df, temp_df])
            del temp_df, temp_t
            gc.collect()
        final_adj_df = final_adj_df.dropna(axis = 1, how='all').fillna(0)
        if strAdj == "overall":
            return final_adj_df
        else:
            temp_set = final_adj_df.groupby([strHH, strMH]).agg('sum')
            df_post_segments = self.creatingSegmentFlags(temp_set.reset_index(),unq_seg, strAdj, hc) #Post Updating the counts, function is called to create segment flags
            return df_post_segments
        
    def creatingSegmentFlags(self, Dataset, unq_seg, strAdj, hc):
        for seg in unq_seg:
            _segLevel = [x for x in Dataset.columns if seg in x]
            _t = [x for x in _segLevel if x.endswith('_t')]
            _c = [x for x in _segLevel if x.endswith('_c')]
            Dataset[seg+'_segment'] = Dataset[_segLevel].sum(axis = 1).map(lambda x: 1 if x>0 else 0)
            Dataset[seg+'_segment_t'] = Dataset[_t].sum(axis = 1).map(lambda x: 1 if x>0 else 0)
            Dataset[seg+'_segment_c'] = Dataset[_c].sum(axis = 1).map(lambda x: 1 if x>0 else 0)
            del _segLevel, _t, _c
            gc.collect()
        self.variableCountSummary(Dataset, unq_seg[0].split('_')[0], strAdj, hc)
        return Dataset
        
    def variableCountSummary(self, Dataset, campaignName, strAdj, hc):
        if strAdj.lower() != "adj" :
            pass
        else:
            count =[]
            campaignCols = [x for x in Dataset.columns if campaignName in x]
            for col in campaignCols:
                count.append(sum(Dataset[Dataset[col]>0][col]))
            dfVarCountReport = pd.DataFrame({"Households":count,"TEST_FLAG":campaignCols})
            self.csvTohive(hc.createDataFrame(dfVarCountReport),strAdj+' Household Count Report', '_'.join(self.expTblName.split('_')[:-1])+'_cntrl_'+strAdj+'_HHCount')
            del count
            
    def IdentifyNumUniqSplits(self, _uniq_prod_ls, _uniq_seg_ls, strMethod): #Function Defination to Generate Segment Wise Report Post Segment Wise Control Adjustment
        _mail_holdout = ['H','M']
        _response_ind = ['0','1']
        if strMethod == "overall":
            _uniq_seg_ls = ['Overall']
        print("Total number of unique keys identified are %d"%(len(_uniq_seg_ls)*len(_uniq_prod_ls)*2*2))
        k = {} #Dictionary that will store unique keys for which segment level report to be generated
        for items in [(a+'SPEND'+b).upper() for a in _uniq_prod_ls for b in _uniq_seg_ls]:
            k[items] = [(a+'_SPEND'+b+'_CBUYER'+c+d).upper() for a in _uniq_prod_ls if a == items.split('SPEND')[0] for b in _uniq_seg_ls if b.upper() == items.split('SPEND')[1] for c in _mail_holdout for d in _response_ind]
        print k
        return k
        
#    def Segment_ovrl_report(self, Dataset, _uniq_prod_ls, _uniq_seg_ls, lsColNames, strMethod):
#        dicKeys = self.IdentifyNumUniqSplits(_uniq_prod_ls, [x.upper() for x in _uniq_seg_ls], strMethod)
#        print "\n Inside Segment report method Dataset shape {0}".format(Dataset.shape)
#        lsFnlValues = []
#        for key, value in dicKeys.iteritems():
#            o_key = createFilterObj4Keys(key, _uniq_seg_ls, strMethod)
#            if strMethod == "segment":
#                subset = Dataset.loc[(Dataset[o_key.seg_var] == 1)][[lsColNames[0],lsColNames[1],o_key.seg_var,o_key.seg_var+'_t',o_key.seg_var+'_c',o_key.prod_var, o_key.response_var, o_key.trip_var]]
#            elif strMethod == "weighted":
#                subset = Dataset.loc[(Dataset[o_key.seg_var] == 1)][[lsColNames[0],lsColNames[1],o_key.seg_var,o_key.seg_var+'_t',o_key.seg_var+'_c',o_key.prod_var, o_key.response_var, o_key.ratio_var, o_key.trip_var]]
#            else:
#                subset = Dataset
##            meanval = subset[o_key.prod_var].mean()
#            c_tab = pd.crosstab(subset[lsColNames[1]], subset[o_key.response_var])
#            chistat, chipval = stats.chi2_contingency(observed= c_tab)[0:2]
#            if strMethod == "weighted":
#                #Generating summary statistics to generate ttest using stats
#                abar = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].mean()
#                avar = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].var(ddof=1)
#                na = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].size
#                adof = na - 1
#                
#                bbar = np.average(subset[subset[lsColNames[1]] == 'H'][o_key.prod_var], weights = subset[subset[lsColNames[1]] == 'H'][o_key.ratio_var])
#                bvar = subset[subset[lsColNames[1]] == 'H'][o_key.prod_var].var(ddof=1)
#                nb = subset[subset[lsColNames[1]] == 'H'][o_key.prod_var].size
#                bdof = nb - 1
#                #Using Manual Formula to generate T-Stat and P-Value
#                tstat = (abar - bbar) / np.sqrt(avar/na + bvar/nb)
#                dof = (avar/na + bvar/nb)**2 / (avar**2/(na**2*adof) + bvar**2/(nb**2*bdof))
#                tpval = 2*stdtr(dof, -np.abs(tstat))
#                del abar, avar, na, bbar, bvar, nb, adof, bdof, dof
#                
#            else:
#                tstat, tpval = stats.ttest_ind(subset[subset[lsColNames[1]] == 'M'][o_key.prod_var], subset[subset[lsColNames[1]] == 'H'][o_key.prod_var], equal_var = False)[0:2]
#            for item in value:
#                o_val = createFilterObj4Val(item, Dataset.columns, strMethod)
#                if strMethod == "segment":
#                    filtered_set = subset.loc[(subset[o_val.subseg_var] == 1) & (subset[lsColNames[1]] == o_val.holdout_flg)]
#                    meanval = filtered_set[o_key.prod_var].mean()
#                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
#                    segmentKey = o_key.seg_var.split('segment')[0]+'Buyer'
#                elif strMethod == "weighted":
#                    filtered_set = subset.loc[(subset[o_val.subseg_var] == 1) & (subset[lsColNames[1]] == o_val.holdout_flg)]
#                    if o_val.holdout_flg.lower() == 'm':
#                        meanval = filtered_set[o_key.prod_var].mean()
#                    else:
#                        meanval = np.average(filtered_set[o_key.prod_var], weights = filtered_set[o_key.ratio_var])
#                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
#                    segmentKey = o_key.seg_var.split('segment')[0]+'Buyer'
#                else:
#                    filtered_set = subset.loc[(subset[lsColNames[1]] == o_val.holdout_flg)]
#                    meanval = filtered_set[o_key.prod_var].mean()
#                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
#                    segmentKey = o_key.seg_var
#                totTrips = filtered_set[o_key.trip_var].sum()
#                sumOfSales = filtered_set[o_key.prod_var].sum()
#                freq = filtered_set.shape[0]
##                if totTrips == 0:
##                    lsFnlValues.append([o_val.holdout_flg,freq,chistat,chipval,float(tstat.astype(float)),tpval,meanval,o_val.response_flg,segmentKey,item])
##                else:
#                lsFnlValues.append([o_val.holdout_flg,freq,chistat,chipval,float(tstat.astype(float)),tpval,meanval,o_val.response_flg,segmentKey,item])
#            del chistat, chipval, tstat, tpval, meanval, segmentKey, o_key, o_val, totTrips, sumOfSales
#            gc.collect()
#        _df = pd.DataFrame(lsFnlValues,columns = ['Mail_Holdout','Frequency','Value','Prob','tValue','tProb','Mean_spend','Resp','Segment','Key'])
#        return _df
        
        ###With Additional metrics
    def Segment_ovrl_report(self, Dataset, _uniq_prod_ls, _uniq_seg_ls, lsColNames, strMethod):
        dicKeys = self.IdentifyNumUniqSplits(_uniq_prod_ls, [x.upper() for x in _uniq_seg_ls], strMethod)
        print "\n Inside Segment report method Dataset shape {0}".format(Dataset.shape)
        lsFnlValues = []
        for key, value in dicKeys.iteritems():
            o_key = createFilterObj4Keys(key, _uniq_seg_ls, strMethod)
            if strMethod == "segment":
                subset = Dataset.loc[(Dataset[o_key.seg_var] == 1)][[lsColNames[0],lsColNames[1],o_key.seg_var,o_key.seg_var+'_t',o_key.seg_var+'_c',o_key.prod_var, o_key.response_var, o_key.trip_var]]
            elif strMethod == "weighted":
                subset = Dataset.loc[(Dataset[o_key.seg_var] == 1)][[lsColNames[0],lsColNames[1],o_key.seg_var,o_key.seg_var+'_t',o_key.seg_var+'_c',o_key.prod_var, o_key.response_var, o_key.ratio_var, o_key.trip_var]]
            else:
                subset = Dataset
#            meanval = subset[o_key.prod_var].mean()
            c_tab = pd.crosstab(subset[lsColNames[1]], subset[o_key.response_var])
            chistat, chipval = stats.chi2_contingency(observed= c_tab)[0:2]
            if strMethod == "weighted":
                #Generating summary statistics to generate ttest using stats
                abar = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].mean()
                avar = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].var(ddof=1)
                na = subset[subset[lsColNames[1]] == 'M'][o_key.prod_var].size
                adof = na - 1
                
                bbar = np.average(subset[subset[lsColNames[1]] == 'H'][o_key.prod_var], weights = subset[subset[lsColNames[1]] == 'H'][o_key.ratio_var])
                bvar = subset[subset[lsColNames[1]] == 'H'][o_key.prod_var].var(ddof=1)
                nb = subset[subset[lsColNames[1]] == 'H'][o_key.prod_var].size
                bdof = nb - 1
                #Using Manual Formula to generate T-Stat and P-Value
                tstat = (abar - bbar) / np.sqrt(avar/na + bvar/nb)
                dof = (avar/na + bvar/nb)**2 / (avar**2/(na**2*adof) + bvar**2/(nb**2*bdof))
                tpval = 2*stdtr(dof, -np.abs(tstat))
                del abar, avar, na, bbar, bvar, nb, adof, bdof, dof
                
            else:
                tstat, tpval = stats.ttest_ind(subset[subset[lsColNames[1]] == 'M'][o_key.prod_var], subset[subset[lsColNames[1]] == 'H'][o_key.prod_var], equal_var = False)[0:2]
            for item in value:
                o_val = createFilterObj4Val(item, Dataset.columns, strMethod)
                if strMethod == "segment":
                    filtered_set = subset.loc[(subset[o_val.subseg_var] == 1) & (subset[lsColNames[1]] == o_val.holdout_flg)]
                    meanval = filtered_set[o_key.prod_var].mean()
                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
                    segmentKey = o_key.seg_var.split('segment')[0]+'Buyer'
                elif strMethod == "weighted":
                    filtered_set = subset.loc[(subset[o_val.subseg_var] == 1) & (subset[lsColNames[1]] == o_val.holdout_flg)]
                    if o_val.holdout_flg.lower() == 'm':
                        meanval = filtered_set[o_key.prod_var].mean()
                    else:
                        meanval = np.average(filtered_set[o_key.prod_var], weights = filtered_set[o_key.ratio_var])
                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
                    segmentKey = o_key.seg_var.split('segment')[0]+'Buyer'
                else:
                    filtered_set = subset.loc[(subset[lsColNames[1]] == o_val.holdout_flg)]
                    meanval = filtered_set[o_key.prod_var].mean()
                    filtered_set = filtered_set[filtered_set[o_key.response_var] == o_val.response_flg]
                    segmentKey = o_key.seg_var
                totTrips = filtered_set[o_key.trip_var].sum()
                sumOfSales = filtered_set[o_key.prod_var].sum()
                freq = filtered_set.shape[0]
                lsFnlValues.append([o_val.holdout_flg,freq,chistat,chipval,float(tstat.astype(float)),tpval,meanval,float(sumOfSales/totTrips),float(totTrips/freq),o_val.response_flg,segmentKey,item])
            del chistat, chipval, tstat, tpval, meanval, segmentKey, o_key, o_val, totTrips, sumOfSales
            gc.collect()
        _df = pd.DataFrame(lsFnlValues,columns = ['Mail_Holdout','Frequency','Value','Prob','tValue','tProb','Mean_spend','Dollars_per_trip','Trips_per_buyer','Resp','Segment','Key'])
        return _df
        
    def segwise_vars_for_weighted_approach(self, segValue, allSubSegments):
        k = [x for x in allSubSegments if (segValue in x) & (x.rsplit('_',1)[0] != segValue)]
        c_types = [x for x in k if x.rsplit('_',1)[1] == 'c']
        t_types = [x for x in k if x.rsplit('_',1)[1] == 't']
        #Creating Dataframe and ensuring the order of subsegment to be appeared in Weighted Report        
        lsnum, lsctype, lsttype, lsSubseg = [], [], [], []
        for x, y in zip(c_types, t_types):
            if x[:-2] == y[:-2]:
                lsnum.append(x[-3:][0])
                lsctype.append(x)
                lsttype.append(y)
                lsSubseg.append(x.rsplit('_',1)[0])
            else:
                lsnum.append(x[-3][0])
                lsctype.append(x)
                lsttype.append([j for j in t_types if x[:-2] in j][0])
                lsSubseg.append(x.rsplit('_',1)[0])
        _df = pd.DataFrame({"num" : lsnum,"ctype" : lsctype, "ttype" : lsttype, "Subsegment" : lsSubseg})\
        .sort('num')[['num', 'Subsegment', 'ttype', 'ctype']]
        print "\n Segment wise _t & _c variables identified as.....This info is to be fed into Weighted Sales Comparison Report"
        print _df
        del k, c_types, t_types, lsnum, lsctype, lsttype, lsSubseg
        gc.collect()
        return _df
    
    def weighted_report_per_segment(self, segWiseVarDf):
        ls = [0,1]
        for index, row in segWiseVarDf.iterrows():
            if str(row.num) == "1":
                df1 = pd.DataFrame({"temp" : 1, row.Subsegment : ls})
            else:
                df2 = pd.DataFrame({"temp" : 1, row.Subsegment : ls})
                fnl_df = pd.merge(df1, df2, on = ["temp"])
                df1 = fnl_df
        fnl_df.drop('temp', axis= 1, inplace = True)
        fnl_df = fnl_df.loc[~(fnl_df==0).all(axis = 1)]
        del ls
        gc.collect()
        return fnl_df
        
    def updating_exp_for_weighted_segments(self, segValue, Dataset, lsSubSegments, prod_hierarchy, campaignName):
        df_segwise_vars = self.segwise_vars_for_weighted_approach(segValue, lsSubSegments)
        df_segwise_report = self.weighted_report_per_segment(df_segwise_vars)
        gc.collect()
        #Updating key and CampaignName in seg wise report
        df_segwise_report['key_'+segValue] = df_segwise_report.apply(lambda x : ''.join(x.astype(str)), axis = 1)
        #Creating Segment wise key in Experian Table for test and control subsegments seperately
        Dataset['key_'+segValue+'_t'] = Dataset[df_segwise_vars.ttype.tolist()].apply(lambda x: ''.join(x.astype(int).astype(str)), axis = 1)
        Dataset['key_'+segValue+'_c'] = Dataset[df_segwise_vars.ctype.tolist()].apply(lambda x: ''.join(x.astype(int).astype(str)), axis = 1)
        
        #Adding Households counts in Segwise Report, and calculating weight in a segment
        t, c = [], [] #Empty lists with test and control households counts
        for index, row in df_segwise_report.iterrows():
            t.append(Dataset[Dataset['key_'+segValue+'_t'] == row['key_'+segValue]].size)
            c.append(Dataset[Dataset['key_'+segValue+'_c'] == row['key_'+segValue]].size)

        df_segwise_report[segValue+'_t_count'], df_segwise_report[segValue+'_c_count'] = t, c
        df_segwise_report[segValue+'_ratio'] = df_segwise_report[[segValue+'_t_count', segValue+'_c_count']].apply(lambda x : float(float(x[0])/float(x[1])) if (int(x[0]) !=0) & (int(x[1]) != 0) else 0, axis = 1)
        del t, c, df_segwise_vars
        print "\nTest to Control ratio for {0}\n".format(segValue)
        df_segwise_report = df_segwise_report.loc[~(df_segwise_report==0).all(axis = 1)]
        df_segwise_report['CampaignName'] = campaignName
        print df_segwise_report
        gc.collect()
        #Updating Experian Table with Update Sales Values by Weight and Returning Adjusted Experian Table, and Segwise Report
        print "\nUpdating Experian for {0} segment regarding Weighted Report....".format(segValue)
        ls_col = [x for x in Dataset.columns]
        ls_col.append(segValue+'_ratio')
        #Merging ratio variable in Dataset, used for calculating weighted means, and ttest
        Dataset = pd.merge(Dataset, df_segwise_report[['key_'+segValue, segValue+'_ratio']], how = 'left', left_on = 'key_'+segValue+'_c', right_on = 'key_'+segValue).fillna(0)
        Dataset = Dataset[ls_col]
        del ls_col
        gc.collect()

        return Dataset, df_segwise_report


class dynamicObjectCreation:
    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)

#Class with functions relevant for Generating Trans variables from Scan Table
class flag(dynamicObjectCreation):
    def __init__(self, tScan, strOprKey, clean_dict, buyerFlgName, tripflgName):
        self.tblScan = tScan
        self.operation = self.func[strOprKey]
        self.dynProp = dynamicObjectCreation(clean_dict)
        self.buyerFlg = buyerFlgName
        self.tripFlg = tripflgName
        
    def func_saledept(self):
        if type(self.dynProp.department) == unicode:
            dept_list = list(ast.literal_eval(self.dynProp.department))
        else:
            dept_list = self.dynProp.department
        totSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
        .where(self.tblScan.acctg_dept_nbr.isin(dept_list))\
        .select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
        .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
        .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
        .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale            
            
    def func_salecatg(self):
        if type(self.dynProp.category) == unicode:
            cat_list = list(ast.literal_eval(self.dynProp.category))
        else:
            cat_list = self.dynProp.category
        totSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
        .where(self.tblScan.dept_category_nbr.isin(cat_list))\
        .select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
        .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
        .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
        .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale
        
    def func_salesubcatg(self):
        if type(self.dynProp.subcategory) == unicode:
            subcat_list = list(ast.literal_eval(self.dynProp.subcategory))
        else:
            subcat_list = self.dynProp.subcategory            
        totSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
        .where(self.tblScan.dept_subcatg_nbr.isin(subcat_list))\
        .select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
        .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
        .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
        .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale
        
    def func_salefineline(self):
        if type(self.dynProp.fineline) == unicode:
            fineline_list = list(ast.literal_eval(self.dynProp.fineline))
        else:
            fineline_list = self.dynProp.fineline            
        totSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
        .where(self.tblScan.fineline_nbr.isin(fineline_list))\
        .select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
        .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
        .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
        .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale
        
    def func_salefineSubcat(self):
        if type(self.dynProp.fineline) == unicode:
            fineline_list = list(ast.literal_eval(self.dynProp.fineline))
        else:
            fineline_list = self.dynProp.fineline
        if type(self.dynProp.subcategory) == unicode:
            subcat_list = list(ast.literal_eval(self.dynProp.subcategory))
        else:
            subcat_list = self.dynProp.subcategory
        totSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
        .where((self.tblScan.fineline_nbr.isin(fineline_list)) & (self.tblScan.dept_subcatg_nbr.isin(subcat_list)))\
        .select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
        .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
        .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
        .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale
            
    def func_saleupc(self):
        if type(self.dynProp.upc) == unicode:
            upc_list = list(ast.literal_eval(self.dynProp.upc))
        else:
            upc_list = self.dynProp.upc
        filterSale = self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
            .select('household_id', 'retail_price', 'upc_nbr', 'store_nbr', 'visit_nbr')\
            .withColumn('upc_present',f.when(f.col('upc_nbr').isin(upc_list),1).otherwise(0))            
        totSale = filterSale.filter(filterSale.upc_present == 1).select('household_id','retail_price', 'store_nbr', 'visit_nbr') \
            .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
            .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
            .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
        return totSale
        
    def func_saleblank(self):
        return self.tblScan.filter(self.tblScan.visit_date.between(self.dynProp.startdate.strftime('%Y-%m-%d'),self.dynProp.enddate.strftime('%Y-%m-%d')))\
            .select('household_id','retail_price', 'store_nbr', 'visit_nbr')\
            .groupBy('household_id').agg(f.sum(f.col('retail_price')), f.countDistinct(f.concat_ws('_','store_nbr','visit_nbr').alias('trip')).alias(str(self.tripFlg)))\
            .withColumn(str(self.dynProp.variableName.lower()),f.col("sum(retail_price)"))\
            .withColumn(str(self.buyerFlg), f.when(f.col("sum(retail_price)") > 0, 1).otherwise(0))
            
    func = dict(
        dept = func_saledept,
        catg = func_salecatg,
        subcatg = func_salesubcatg,
        fineline = func_salefineline,
        finesub = func_salefineSubcat,
        upc = func_saleupc,
        blank = func_saleblank
    )

#function to identify and remove Nan while reading excel sheets.        
def isNaN(num):
    return num !=num

#Setting up Spark and Hive Contexts
def main(argv):
    try:
        #####------------------------------------Validating Input sheet-----------------------------------------------###
        assert len(argv) >= 3, "Command Line arguments passed not meeting minimum criteria of ExerianTableName, ScanTableName and Yes/No/All Flag for weighted Report !"
        print "Tracking Input passed as"
        print "Experian Table : {0}\nScan Table : {1} \nWeighted Report Required : {2}\n".format(argv[1], argv[2], argv[3])
        sc = SparkContext(appName="Tracking for WMT Direct campaign")    
    
        hc = HiveContext(sc)
        pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
        dicInput = pd.read_excel("TrackingInput.xlsx")
        objCodeValidation = codeValidation()
        if not objCodeValidation.functoValidate(dicInput):
            raise ValueError('Something not right in TrackingInput.xlsx sheet, please check....Code terminating')
        del objCodeValidation
        gc.collect()      
        objExp = ExperianMerge(argv[1],argv[2], dicInput['enddate'].tolist()[1], dicInput['startdate'].tolist()[1])
        dicInput = pd.read_excel("TrackingInput.xlsx").T.to_dict()  
        #Setting up database
        hc.sql('USE CKPIRI') 
  #####----------------------------STEP 1 - Merging Experian Files and updating test control set------------------------###        
        if (argv[3].lower() == 'no') or (argv[3].lower() == 'all') or (len(argv) == 3):
#            tbl_mail_ctrl = hc.createDataFrame(objExp.testCtrlMerge(hc))
#            gc.collect()
#            print "Saving Adjusted Mail_Control Table to CKPIRI, Name -- %s"%('_'.join(argv[1].split('_')[:-1])+'_Exp')
#            tbl_mail_ctrl.fillna(0).write.mode('overwrite').saveAsTable('_'.join(argv[1].split('_')[:-1])+'_Exp',format='orc')
#            del tbl_mail_ctrl
#            gc.collect()
#            gc.collect()
        
        ###comment from line 565 till 594 for trans logic
#    #####--------------------------STEP 2 - Rolling up scan data at test/cntrl Households, and creating Total_Spend & Buyer flags-----------------###        
            query = "select household_id as hhid from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Exp')
            tbl_E = hc.sql(query)
            tbl_E_S = objExp.scnMergeExp(hc, tbl_E)
            tbl_E_S.select(tbl_E_S.household_id).distinct().registerTempTable('tracking_temp_table')
            for items in dicInput:
                dicInput[items]['variableName'] = items
            #####--------------------------creating Total Spend Flags and Buyer Flags-----------------------------------###    
            for items in dicInput:
                gc.collect()
                clean_dict = {k: dicInput[items][k] for k in dicInput[items] if not isNaN(dicInput[items][k])}
                k_item_for_map = tuple(x for x in clean_dict.keys() if x not in ['enddate', 'startdate', 'operation','variableName', 'timeperiod'])
                if len(k_item_for_map) == 0:
                    strOpr = 'blank'
                else:
                    strOpr = ms.keysMapping[k_item_for_map]
                buyerflgName, tripflgName = 'buyer_'+items.split('_')[2].lower(), 'trip_'+items.split('_')[2].lower()
                print("Generating sales flag %s & buyer flag %s & trip flag %s for Tracking...."%(items,buyerflgName, tripflgName))
                obj_oprClass = flag(tbl_E_S, strOpr, clean_dict, buyerflgName, tripflgName)
                objExp.mergingTbl(obj_oprClass.operation(obj_oprClass), clean_dict["variableName"].lower(), buyerflgName, tripflgName, hc)
                del obj_oprClass
                gc.collect()
                         
            tbl_flg_exp = hc.sql('select * from tracking_temp_table')
            print "Saving Households found in scan and respective flags Table to CKPIRI, Name -- %s"%('_'.join(argv[1].split('_')[:-1])+'_Trans')
            tbl_flg_exp.fillna(0).write.mode('overwrite').saveAsTable('_'.join(argv[1].split('_')[:-1])+'_Trans',format='orc')
            hc.dropTempTable('tracking_temp_table')
            del tbl_flg_exp, tbl_E_S
            gc.collect()
            gc.collect()      
            
    #####----------------------------- STEP 3 - Generating Descriptive Statistics for Flags --------------------------------------------------------###
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Exp')
            tbl_exp = hc.sql(query) #Pulling Adjusted Experian table
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Trans')
            tbl_flags = hc.sql(query) #Pulling Rolled up test/control Households with Total_spend and buyer flags
            tbl_flags = tbl_flags.withColumnRenamed('household_id', 'hhid')
            ls_final_col = tbl_exp.columns + tbl_flags.columns[1:]
            
            #Merging experian table with flags tables
            print "Merging Experian table and Flag tables, and converting to Pandas Dataframe....."
            merged_tbl = tbl_exp.join(tbl_flags, f.col(ls_final_col[0]) == f.col('hhid'), 'left').select(ls_final_col).fillna(0).toPandas()
            merged_tbl.columns = map(str.lower, merged_tbl.columns)
            segments = [x for x in merged_tbl.columns if ((x.lower().endswith('_t')) or (x.lower().endswith('_c')))]
            prod_hierarchy = [x.lower() for x in dicInput.keys()]
            print "Creating Test Flag Report........"
            #Calling Function to create report and save in xlsx
            dftestFlgReport = objExp.testFlagTrackingReport(merged_tbl, prod_hierarchy, segments)
            objExp.csvTohive(hc.createDataFrame(dftestFlgReport),'Test Flag Report', '_'.join(argv[1].split('_')[:-1])+'_TestflagReport')
    
            del dftestFlgReport, tbl_exp, tbl_flags, query
            gc.collect()
            print "Creating Test Flag Household Count Report......."
            dftestFlgHHcountReport = objExp.testFlagHHCountReport(merged_tbl, segments)
            objExp.csvTohive(hc.createDataFrame(dftestFlgHHcountReport),'Test Flag HH Count report', '_'.join(argv[1].split('_')[:-1])+'_exp_HH_countReport')
    
            del dftestFlgHHcountReport
            gc.collect()
            
    #####------------------------------------------------ Step 4 - Control Adjustement for Segment-----------------------------------------------------###        
            
            #identifying unique segments
            col_n = []
            [col_n.append(z) for z in ['_'.join(y.split('_')[:-2]) for y in segments] if z not in col_n]
            uniq_segments = col_n[:]
            for item in col_n:
                if sum([item in x for x in col_n]) >=2:
                    uniq_segments.remove(item)
            col_n = []
            [col_n.append(z) for z in ['_'.join(y.split('_')[:-1]) for y in segments] if z not in col_n]
            uniq_subsegments = col_n[:]
            del col_n
            print "____________%d -- are the count of Unique segments identified, i.e %s_________________"%(len(uniq_segments), uniq_segments)
            print "____________%d -- are the count of Unique Sub Segments identified, i.e %s_________________"%(len(uniq_subsegments), uniq_subsegments)
            gc.collect()
    
            dfAdjControlReport, df_Adj_SS, df_NonAdj_SS = objExp.adjustMailControlGrpSegment(merged_tbl, segments, uniq_segments, uniq_subsegments)
            print "\nAdjusted Count Data Frame\n"
            print df_Adj_SS,"\n"
            print "Non-adjusted count Dataframe\n"
            print df_NonAdj_SS
            df_adjusted_experian = objExp.creatingAdjustedExpTbl(merged_tbl, df_Adj_SS, uniq_segments, 'Adj', hc)
            objExp.csvTohive(hc.createDataFrame(df_adjusted_experian),'Adjusted Experian Table', '_'.join(argv[1].split('_')[:-1])+'_Seg_Adj_Exp')
            del df_Adj_SS, df_adjusted_experian
            gc.collect()
            
            merged_tbl = objExp.creatingSegmentFlags(merged_tbl, uniq_segments, "NonAdj", hc)
            df_nonAdj_SegmentWiseReport = objExp.Segment_ovrl_report(merged_tbl, [x.split('_')[2].upper() for x in prod_hierarchy], [x.lower() for x in uniq_segments], [x.lower() for x in ls_final_col[:2]], "segment")
            objExp.csvTohive(hc.createDataFrame(df_nonAdj_SegmentWiseReport),'Non Adjusted Segment wise Report', '_'.join(argv[1].split('_')[:-1])+'_NonAdj_SegTest_report')
            
            df_nonAdj_OverallWiseReport = objExp.Segment_ovrl_report(merged_tbl, [x.split('_')[2].upper() for x in prod_hierarchy], [x.lower() for x in uniq_segments], [x.lower() for x in ls_final_col[:2]], "overall")
            objExp.csvTohive(hc.createDataFrame(df_nonAdj_OverallWiseReport),'Non Adjusted Overall wise Report', '_'.join(argv[1].split('_')[:-1])+'_NonAdj_Overall_report')
            #df_non_adj_experian = objExp.creatingAdjustedExpTbl(merged_tbl, df_NonAdj_SS, uniq_segments, 'NonAdj', hc)
            #objExp.csvTohive(hc.createDataFrame(df_non_adj_experian),'Non Adjusted Experian Table', '_'.join(argv[1].split('_')[:-1])+'_NonAdjusted_Exp')
            del df_NonAdj_SS
            gc.collect()
               
            objExp.csvTohive(hc.createDataFrame(dfAdjControlReport),'Adjusted Control Group Table', '_'.join(argv[1].split('_')[:-1])+'_SegWise_AdjControl')
            del dfAdjControlReport, merged_tbl, ls_final_col, df_nonAdj_OverallWiseReport, df_nonAdj_SegmentWiseReport
            gc.collect()        
    ######------------------------------------------------ Step 5 - Segment Reporting After Segment Adjustement--------------------------------------------------###
            
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Seg_Adj_Exp')
            tbl_adj_exp = hc.sql(query) #Pulling Adjusted Experian table
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Trans')
            tbl_flags = hc.sql(query) #Pulling Rolled up test/control Households with Total_spend, buyer, trip flags
            tbl_flags = tbl_flags.withColumnRenamed('household_id', 'hhid')
            ls_final_col = tbl_adj_exp.columns + tbl_flags.columns[1:]
            print "Merging Adjusted Experian table and Flag tables post Segment Level Adjustment, and converting to Pandas Dataframe....."
            merged_tbl = tbl_adj_exp.join(tbl_flags, f.col(ls_final_col[0]) == f.col('hhid'), 'left').select(ls_final_col).fillna(0).toPandas()
            
        ######-------------------------------------------Step 5.1 - Segment Level Report across products and Subsegments---------------------------------------###
            
            dfSegmentWiseReport = objExp.Segment_ovrl_report(merged_tbl, [x.split('_')[2].upper() for x in prod_hierarchy], [x.lower() for x in uniq_segments], ls_final_col[:2], "segment")
            objExp.csvTohive(hc.createDataFrame(dfSegmentWiseReport),'Adjusted Segment wise Report', '_'.join(argv[1].split('_')[:-1])+'_SegTest_report')
            
            del merged_tbl, ls_final_col, query, dfSegmentWiseReport, segments
            gc.collect()
    
    #####------------------------------------------------ Step 6 - Control Adjustement Overall ------------------------------------------------------------###
    
            segments = [x for x in tbl_adj_exp.columns if ((x.lower().endswith('_t')) or (x.lower().endswith('_c')))]        
            uniq_subsegments = [x for x in segments if 'segment' in x] #This is subsegment level for overall
            dfAdjControlReportOverll, df_Adj_SS_Overll = objExp.adjustMailControlGrpOverall(tbl_adj_exp.toPandas(), uniq_segments, uniq_subsegments)
            print "#############::::::::::::::::::::: Overall Adjusted Control Report @ Overall Level :::::::::::::::::::####################\n"
            print dfAdjControlReportOverll
            print "\n############::::::::::::::::::::: Count of Overall Adjusted Control @ Segment Level :::::::::::::::::####################\n"
            print df_Adj_SS_Overll[['Segment', 'SubSegment', 'FinalSize']]
            objExp.csvTohive(hc.createDataFrame(dfAdjControlReportOverll),'Overall wise Report', '_'.join(argv[1].split('_')[:-1])+'_Overall_AdjControl')
            df_adjusted_experian_overll = objExp.creatingAdjustedExpTbl(tbl_adj_exp.toPandas(), df_Adj_SS_Overll, uniq_segments, 'overall', hc)
            objExp.csvTohive(hc.createDataFrame(df_adjusted_experian_overll),'Overall Adjusted Experian Table', '_'.join(argv[1].split('_')[:-1])+'_Ovrl_Adj_Exp')
            del df_Adj_SS_Overll, df_adjusted_experian_overll, tbl_adj_exp
            gc.collect()
            
        #####-------------------------------------------Step 6.1 - Overall Level Report across products and Segments---------------------------------------###
            
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Ovrl_Adj_Exp')
            tbl_adj_exp = hc.sql(query) #Pulling Adjusted Experian table post Overall Adjustment
            ls_final_col = tbl_adj_exp.columns + tbl_flags.columns[1:]
            print "Merging Adjusted Experian table and Flag tables Post Overall Adjustment, and converting to Pandas Dataframe....."
            merged_tbl = tbl_adj_exp.join(tbl_flags, f.col('household_id') == f.col('hhid'), 'left').select(ls_final_col).fillna(0).toPandas()
            merged_tbl.columns = map(str.lower, merged_tbl.columns)
            
            dfOverallWiseReport = objExp.Segment_ovrl_report(merged_tbl, [x.split('_')[2].upper() for x in prod_hierarchy], [x.lower() for x in uniq_segments], ['household_id','mail_holdout'], "overall")
            objExp.csvTohive(hc.createDataFrame(dfOverallWiseReport),'Segment wise Report', '_'.join(argv[1].split('_')[:-1])+'_Overall_report')
            
            del merged_tbl, ls_final_col, query, dfOverallWiseReport, tbl_adj_exp, segments, tbl_flags, uniq_segments, prod_hierarchy
            gc.collect()
            
            if (argv[3].lower() == 'no') or (len(argv) == 3):
                print "############### :::::::::::: TABLES Created are ::::::::::::::::: ####################\n"
                print "1) %s_Exp - Adjusted Mail_Control Table as per Experian count\n"%('_'.join(argv[1].split('_')[:-1]))
                print "2) %s_Trans - Transaction table at Household Level\n"%('_'.join(argv[1].split('_')[:-1]))
                print "3) %s_Seg_Adj_Exp - Segment Wise Adjusted Experian Table\n"%('_'.join(argv[1].split('_')[:-1]))
                print "4) %s_Ovrl_Adj_Exp - Overll Adjusted Experian Table\n"%('_'.join(argv[1].split('_')[:-1]))
                
                print "############### :::::::::::: REPORTS Generated are :::::::::::::::: ####################\n"
                print "1) %s_TestflagReport - Test Flag Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "2) %s_exp_HH_countReport - Experian Household count Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "3) %s_cntrl_Adj_HHcount - Post Segment Adjusted Household Count Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "4) %s_SegWise_AdjControl - Segment Wise Adjusted Control Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "5) %s_NonAdj_SegTest_report - Non Adjusted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "6) %s_SegTest_report - Adjusted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "7) %s_Overall_AdjControl - Overall Adjusted Control Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "8) %s_NonAdj_Overall_report - Non Adjusted Overall wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "9) %s_Overall_report - Overall report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
        
#####------------------------------------------------ Step 7 - Weighted Control Adjustment Segment Wise Approach ------------------------------------------------------------###
        elif (argv[3].lower() == 'all') or (argv[3].lower() == 'yes'):
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Exp')
            tbl_exp = hc.sql(query) #Pulling Adjusted Experian table
            query = "select * from {}".format('_'.join(argv[1].split('_')[:-1]) +'_Trans')
            tbl_flags = hc.sql(query) #Pulling Rolled up test/control Households with Total_spend, buyer, trip flags
            tbl_flags = tbl_flags.withColumnRenamed('household_id', 'hhid')
            ls_final_col = tbl_exp.columns + tbl_flags.columns[1:]
            #Merging experian table with flags tables
            print "Merging Experian table and Flag tables for Weighted Approach, and converting to Pandas Dataframe....."
            merged_tbl = tbl_exp.join(tbl_flags, f.col('household_id') == f.col('hhid'), 'left').select(ls_final_col).fillna(0).toPandas()
            merged_tbl.columns = map(str.lower, merged_tbl.columns)
            segments = [x for x in merged_tbl.columns if ((x.lower().endswith('_t')) or (x.lower().endswith('_c')))]
            prod_hierarchy = [x.lower() for x in dicInput.keys()]
            ls_final_col = merged_tbl.columns
            gc.collect()
            
            #identifying unique segments
            col_n = []
            [col_n.append(z) for z in ['_'.join(y.split('_')[:-2]) for y in segments] if z not in col_n]
            uniq_segments = col_n[:]
            for item in col_n:
                if sum([item in x for x in col_n]) >=2:
                    uniq_segments.remove(item)
            del col_n
            
            #uniq_segments, uniq_subsegments, prod_hierarchy
            uniq_segments = [x.lower() for x in uniq_segments]
            
            print "\nDataset shape before creating segments {0}".format(merged_tbl.shape)
            
            #Adding segments column to the Experian
            merged_tbl = objExp.creatingSegmentFlags(merged_tbl, uniq_segments, "weighted", hc)        
            
            print "\nDataset shape after creating segments {0}".format(merged_tbl.shape)
            
            for seg in uniq_segments:
                if seg == uniq_segments[0]:
                    merged_tbl, df_seg_report = objExp.updating_exp_for_weighted_segments(seg, merged_tbl, segments, prod_hierarchy, '_'.join(argv[1].split('_')[:-1]))
                else:
                    merged_tbl, df_seg_report_2 = objExp.updating_exp_for_weighted_segments(seg, merged_tbl, segments, prod_hierarchy, '_'.join(argv[1].split('_')[:-1]))
                    df_seg_report = pd.merge(df_seg_report, df_seg_report_2, on = ['CampaignName'])
                gc.collect()
            
            objExp.csvTohive(hc.createDataFrame(df_seg_report),'Weighted Control Adjustment Report', '_'.join(argv[1].split('_')[:-1])+'_Weighted_Adj_report')
            objExp.csvTohive(hc.createDataFrame(merged_tbl[['household_id']+[x for x in merged_tbl.columns if 'ratio' in x]]),'Weighted Experian Table', '_'.join(argv[1].split('_')[:-1])+'_Wtd_Exp')
            del df_seg_report
            gc.collect()
            
            ######-------------------------------------------Step 7.1 - Weighted Control Adjustment Segment Level Report across products and Subsegments---------------------------------------###
            
            dfSegmentWiseReport = objExp.Segment_ovrl_report(merged_tbl, [x.split('_')[2].upper() for x in prod_hierarchy], [x.lower() for x in uniq_segments], ['household_id','mail_holdout'], "weighted")
            objExp.csvTohive(hc.createDataFrame(dfSegmentWiseReport),'Segment wise Report', '_'.join(argv[1].split('_')[:-1])+'_Wtd_SegTest_report')
            
            if argv[3].lower() == 'all':
                print "############### :::::::::::: TABLES Created are ::::::::::::::::: ####################\n"
                print "1) %s_Exp - Adjusted Mail_Control Table as per Experian count\n"%('_'.join(argv[1].split('_')[:-1]))
                print "2) %s_Trans - Transaction table at Household Level\n"%('_'.join(argv[1].split('_')[:-1]))
                print "3) %s_Seg_Adj_Exp - Segment Wise Adjusted Experian Table\n"%('_'.join(argv[1].split('_')[:-1]))
                print "4) %s_Ovrl_Adj_Exp - Overll Adjusted Experian Table\n"%('_'.join(argv[1].split('_')[:-1]))
                print "5) %s_Wtd_Exp - Weighted Experian Table"%('_'.join(argv[1].split('_')[:-1]))
                
                print "############### :::::::::::: REPORTS Generated are :::::::::::::::: ####################\n"
                print "1) %s_TestflagReport - Test Flag Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "2) %s_exp_HH_countReport - Experian Household count Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "3) %s_cntrl_Adj_HHcount - Post Segment Adjusted Household Count Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "4) %s_SegWise_AdjControl - Segment Wise Adjusted Control Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "5) %s_NonAdj_SegTest_report - Non Adjusted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "6) %s_SegTest_report - Adjusted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "7) %s_Overall_AdjControl - Overall Adjusted Control Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "8) %s_NonAdj_Overall_report - Non Adjusted Overall wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "9) %s_Overall_report - Overall report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "10) %s_Weighted_Adj_report - Weighted Control Adjustment Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "11) %s_Wtd_SegTest_report - Weighted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "\n ---------------------Code Terminated Successfully---------------------"
            else:
                print "############### :::::::::::: TABLES Created are ::::::::::::::::: ####################\n"
                print "1) %s_Wtd_Exp - Weighted Experian Table"%('_'.join(argv[1].split('_')[:-1]))
                print "\n############### :::::::::::: REPORTS Generated are :::::::::::::::: ####################\n"
                print "1) %s_Weighted_Adj_report - Weighted Control Adjustment Report\n"%('_'.join(argv[1].split('_')[:-1]))
                print "2) %s_Wtd_SegTest_report - Weighted Segment wise report containing t-test and chisq test statistics\n"%('_'.join(argv[1].split('_')[:-1]))
                print "\n ---------------------Code Terminated Successfully---------------------"            
        else:
            print "User passed Invalid Third command line argument as Input....Code Terminating"
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