# -*- coding: utf-8 -*-
"""
Created on Mon Sep 04 14:59:15 2017
Module - Test Design
@author: vn0bz25
"""
#Importing required Libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.types as typ
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark import StorageLevel
import pandas as pd
import numpy as np
import traceback
import sys
import gc
import ast

class UpdatingInputPlan():
    def __init__(self, tbl_t, tbl_s, tbl_ss, argv_input):
        print "Updating Inputted test plan sheet to required format"
        self.testPlan = tbl_t
        self.segmentPlan = tbl_s
        self.updInputTestPlan()
        self.dictSegment = self.updInputSegmentPlan()
        self.dictSubSegment = self.updInputSubSegmentPlan(tbl_ss, argv_input)
        del tbl_t, tbl_s, self.segmentPlan
    
    def updInputTestPlan(self):
        ls_cols = ('Objective','Segment', 'Model', 'TM Spend Deciles', 'WMT Share Deciles', 'WMT Spend Deciles', 'WMT Banner Deciles', 'CPN_DMR\n(1 or 0)',\
'Test Code', 'Experian Cell Code', 'Available Mail Counts', 'Available Control Counts', 'Desired Mail Counts', 'Desired Control Counts',\
'% Control', 'Actual Mail Counts', 'Actual Control Counts', 'Mail Check', 'Ctrl Check')
        self.testPlan.rename(columns = {k:v for k, v in zip(self.testPlan.columns, ls_cols)}, inplace = True)
        del ls_cols
        self.testPlan = self.testPlan[list(self.testPlan.columns)[:list(self.testPlan.columns).index('Ctrl Check') + 1]]
        self.testPlan.drop(['Objective', 'Model', '% Control', 'Mail Check', 'Ctrl Check'], axis = 1, inplace = True)
        for item in (self.testPlan.Segment.unique()):
            if item == item:
                i = self.testPlan.Segment[self.testPlan.Segment == item].index
                i = i.insert(0,min(i) - 1)
                segName  = self.testPlan['Test Code'][min(i)]
                self.testPlan.ix[i, 'Segment_name'] = segName
            else:
                pass    
        self.testPlan = self.testPlan.dropna(axis = 0, how = 'any', thresh = 10)
        
    def updInputSegmentPlan(self):
        ls_cols = ('Logic', 'Mail', 'Holdout', 'NotRequired1', 'NotRequired2', 'NotRequired3', 'NotRequired4')
        self.segmentPlan.rename(columns = {k:v for k, v in zip(self.segmentPlan.columns, ls_cols)}, inplace = True)
        self.segmentPlan = self.segmentPlan[list(self.segmentPlan.columns)[:list(self.segmentPlan.columns).index('Holdout') + 1]].fillna(0)
        self.segmentPlan = self.segmentPlan.loc[~(self.segmentPlan==0).all(axis = 1)].reset_index().drop('index', axis = 1)
        segNames = self.segmentPlan.loc[~(self.segmentPlan==0).any(axis = 1)].copy()
        print "\n-----------Segment wise count in Report-----------"
        print segNames
        #Looping across segment and creating dictionary with relevant information
        d = dict.fromkeys(segNames.Logic.tolist())
        for item in segNames.Logic.tolist():
            d[item] = dict.fromkeys(['MailCount', 'HoldoutCount', 'Logic'])
            i = int(self.segmentPlan[self.segmentPlan.Logic == item].index.values)
            if list(segNames.index).index(i)-len(list(segNames.index)) + 1 != 0:
                j = segNames.index[list(segNames.index).index(i)-len(list(segNames.index)) + 1]
            else:
                j = self.segmentPlan.index.values[-1] + 1
            d[item]['MailCount'] = int(self.segmentPlan.ix[i,'Mail'])
            d[item]['HoldoutCount'] = int(self.segmentPlan.ix[i, 'Holdout'])
            d[item]['Logic'] = dict(zip([x.split('=')[0] for x in self.segmentPlan.ix[i+1:j-1,'Logic'].tolist()], \
            [x.split('=')[1] for x in self.segmentPlan.ix[i+1:j-1,'Logic'].tolist()]))
            
        return d
        
    def updInputSubSegmentPlan(self, tbl_ss, argv_input):
#        cmpn_name = argv_input.split('_')[0] + '_Segment'
        tbl_ss.loc[-1] = ['Segment_Col_Name', argv_input.split('_')[1] + '_segment']
        dict_ss = dict.fromkeys(tbl_ss.Cross_Tab_Values)
        for item in dict_ss:
            dict_ss[item] = str(tbl_ss[tbl_ss['Cross_Tab_Values'] == item]['Test_Plan_Values'].tolist()[0])
        
        return dict_ss
        
        
class TestPlanSegValues():
    def __init__(self, testPlan, dict_seg, dict_subseg, strFilter, argv_input):
        self.cmpn_name = argv_input.split('_')[1]
        self.dict_tstPlan = self.createDictionaryForTstPlan(testPlan, dict_seg, dict_subseg, strFilter)
        print "\n--------------------------------------------------- Step 2 : Updating data with respect to Test Plan --------------------------------------------------------------------\n"
    #Delete Level for dictionary    
    def removeLevel(self, d, level):
        if type(d) != type({}):
            return d
    
        if level == 0:
            removed = {}
            for k, v in d.iteritems():
                if type(v) != type({}):
                    continue
                for kk, vv in v.iteritems():
                    removed[kk] = vv
            return removed
    
        removed = {}
        for k, v in d.iteritems():
            removed[k] = self.removeLevel(v, level-1)
        return removed
        
    def stripNone(self, data):
        if isinstance(data, dict):
            return {k:self.stripNone(v) for k, v in data.iteritems() if k is not None and v is not None}
        else:
            return data
            
    def createDictionaryForTstPlan(self, testPlan, dict_seg, dict_subseg, strFilter):
        #Creating keys of dictionary as segment values
        _dtplan_ = dict.fromkeys(dict_seg.keys())
        #creating subsegments values against each segment by dictionary of dictionary
        for item in _dtplan_:
            _dtplan_[item] = dict.fromkeys(testPlan[testPlan['Segment'] == item]['Test Code'].tolist())
            for subseg in _dtplan_[item]:
                _dtplan_[item][subseg] = dict.fromkeys(dict_subseg.keys())
                for colName in _dtplan_[item][subseg]:
                    if colName != self.cmpn_name+'_segment':
                        strFl = testPlan[(testPlan['Segment'] == item) & (testPlan['Test Code'] == subseg)][str(dict_subseg[colName])].tolist()[0]
                        if(strFl != strFilter):
                            if type(strFl) == unicode:
                                strFl = ast.literal_eval(strFl)
                                if (type(strFl) == int) or (type(strFl) != tuple) or (type(strFl) == str):
                                    strFl = [str(strFl)]
                                else:
                                    strFl = [str(x) for x in strFl]
                            else:
                                strFl = [str(strFl)]
                            _dtplan_[item][subseg][colName] = strFl
                        else:
                            pass
                    else:
                        _dtplan_[item][subseg][self.cmpn_name+'_segment'] = [str(item)]
        return self.stripNone(self.removeLevel(_dtplan_, 0))
        
    def returnSampledSubSegment(self, tbl, strFlg, ff):
        for i, r in tbl.iterrows():
            if not r[strFlg]:
                ff[i] = ((0, float(r['Desired Mail Counts'])/float(r['Available Mail Counts'])), (1, float(r['Desired Control Counts'])/float(r['Available Control Counts'])))
            else:
                ff[i] = ((0, float(r['Desired Mail Counts'])/float(r['Available Mail Counts'])), (1, 1.0))
        return ff
        
    def identifyingSubsegmentSample(self, sample_tbl, test_code):
        sample_tbl.drop(test_code, inplace = True)
        ff = {}
        sample_tbl['M_flg'], sample_tbl['C_flg'] = sample_tbl['Desired Mail Counts'].apply(lambda x: True if type(x) == unicode and x.split(' ')[1][:-1].lower() == 'brules' else (False if type(x) == int else 'NotApplicable')),\
            sample_tbl['Desired Control Counts'].apply(lambda x: True if type(x) == unicode and x.split(' ')[1][:-1].lower() == 'brules' else (False if type(x) == int else 'NotApplicable'))
        if (all(sample_tbl.ix[:,'M_flg'].tolist())) and (all(sample_tbl.ix[:,'C_flg'].tolist())):
            return True
        else:
            compare_lists = lambda x, y: 'greater' if sum(x) > sum(y) else 'smaller' if sum(x) < sum(y) else 'equal' if sum(x) == sum(y) else False
            return {
            'smaller' : self.returnSampledSubSegment(sample_tbl.ix[sample_tbl['C_flg'] == False, :], 'M_flg', ff),
            'greater' : self.returnSampledSubSegment(sample_tbl.ix[sample_tbl['M_flg'] == False, :], 'C_flg', ff),
            'equal' : self.returnSampledSubSegment(sample_tbl.ix[sample_tbl['C_flg'] == False, :], 'M_flg', ff),
            False : False
            }[compare_lists(sample_tbl['M_flg'].tolist(), sample_tbl['C_flg'].tolist())]
            
        
    def creatingMailControlGrpDictionary(self, str_holdout_flag):
        d_mail_cntrl_flags = dict.fromkeys(self.dict_tstPlan.keys())
        for item in d_mail_cntrl_flags:
            d_mail_cntrl_flags[item] = {item+'_t' : {item : '1', str_holdout_flag : '0'}, item+'_c' : {item : '1', str_holdout_flag : '1'}}
        return self.removeLevel(d_mail_cntrl_flags, 0)
        
    def creatingSampleDictionary(self, testplan):
        d_sampling = dict.fromkeys([x for x in self.dict_tstPlan.keys() if 'brules' in x.rsplit('_',1)[1].lower()])
        for item in d_sampling:
            extra_t = self.identifyingSubsegmentSample(testplan.ix[testplan['Segment'] == testplan.ix[testplan['Test Code'] == item, 'Segment'].tolist()[0], ('Test Code', 'Desired Mail Counts', 'Desired Control Counts', 'Available Control Counts', 'Available Mail Counts')].set_index('Test Code'), item)
            if type(extra_t) == dict:
                d_sampling[item] = ((1, float(testplan.ix[testplan['Test Code'] == item, 'Desired Control Counts'])/float(testplan.ix[testplan['Test Code'] == item, 'Available Control Counts'])),\
                (0, float(testplan.ix[testplan['Test Code'] == item, 'Desired Mail Counts'])/float(testplan.ix[testplan['Test Code'] == item, 'Available Mail Counts'])), \
                extra_t)
            else:
                d_sampling[item] = ((1, float(testplan.ix[testplan['Test Code'] == item, 'Desired Control Counts'])/float(testplan.ix[testplan['Test Code'] == item, 'Available Control Counts'])),\
                (0, float(testplan.ix[testplan['Test Code'] == item, 'Desired Mail Counts'])/float(testplan.ix[testplan['Test Code'] == item, 'Available Mail Counts'])))
        gc.collect()
        return d_sampling
        
    def generatingSamples(self, tbl, dict_sampling):
        i_flg = 0
        for k, v in dict_sampling.iteritems():
            if (v[0][1] == 1) and (v[1][1] == 1):
                smp_tbl = tbl.filter(f.col(k) == 1)
            else:
                smp_tbl = tbl.filter(f.col(k) == 1).sampleBy('holdout_flag', fractions = {v[0][0]:v[0][1], v[1][0]:v[1][1]}, seed = 12345678)
            if i_flg == 0:
                fnl_tbl = smp_tbl
                del smp_tbl
                gc.collect()
                i_flg += 1
            else:
                fnl_tbl = fnl_tbl.unionAll(smp_tbl)
                del smp_tbl
                gc.collect()
        return fnl_tbl
        
    def updatingSubSegments_non_brules(self, tbl, d_subsegments):
#        ls_non_brules_seg = [k.rsplit('_',1)[0] for k, v in dict_sampling.iteritems() if len(v) > 2]
#        d = {k: d_subsegments[k] for k in d_subsegments.keys() if k.rsplit('_',1)[0] not in ls_non_brules_seg}
        return tbl.select(["*"] +
                    [
                        eval('&'.join([
                            '(tbl["' + c + '"].isin(["' + '","'.join(v) + '"]))' for c, v in d_subsegments[sk].iteritems()
                         ])).cast("int").alias(sk) 
                        for sk in d_subsegments.keys()
                    ]
            )
        
    def updatingSubSegments_brules(self, tbl, testplan, segname):
        print "\n--------------------------------------------------- Step 3 : Creating Brules Sub-Segments --------------------------------------------------------------------\n"
        str_holdout_flag = [x for x in tbl.columns if 'holdout' in x.lower() and 'flag' in x.lower()][0]
        d_brules = {k : self.dict_tstPlan[k] for k in self.dict_tstPlan.keys() if 'brules' in k.lower()}
        d_subsegments = {k : self.dict_tstPlan[k] for k in self.dict_tstPlan.keys() if 'brules' not in k.lower()}
#        print [
#                        eval('&'.join([
#                            '(tbl["' + c + '"].isin(["' + '","'.join(v) + '"]))' for c, v in d_brules[sk].iteritems()
#                         ])).cast("int").alias(sk) 
#                        for sk in d_brules.keys()
#                    ]
        #Creating subsegments for each segment iteratively, using eval operator to merge each filter condition by & operation.
        print d_brules #prints brules dictionary on output screen
        temp_df = tbl.select(["*"] +
                    [
                        eval('&'.join([
                            '(tbl["' + c + '"].isin(["' + '","'.join(v) + '"]))' for c, v in d_brules[sk].iteritems()
                         ])).cast("int").alias(sk) 
                        for sk in d_brules.keys()
                    ]
            )

        dict_sampling = self.creatingSampleDictionary(testplan)
        print "\n--------------------------------------------------- Step 4 : Updating Brules Sub-Segments as Sample Size --------------------------------------------------------------------\n"
        #returning sampled set of brules
        print dict_sampling #prints sampling dictionary on output screen
        temp_df = self.generatingSamples(temp_df, dict_sampling)
        #Updating sub segments other than brules where desired counts passed as [In Brules]
        print "\n--------------------------------------------------- Step 5 : Creating Sub-Segments other than Brules --------------------------------------------------------------------\n"
        print d_subsegments
        temp_df = temp_df.select(["*"] +
                    [
                        eval('&'.join([
                            '(temp_df["' + c + '"].isin(["' + '","'.join(v) + '"]))' for c, v in d_subsegments[sk].iteritems()
                         ])).cast("int").alias(sk) 
                        for sk in d_subsegments.keys()
                    ]
            )
        print "\n--------------------------------------------------- Step 6 : Updating Sub-Segments other than Brules as Sample Size --------------------------------------------------------------------\n"
        #Generating Samples for Sub Segments where numeric count is provided and not like [In Brules]
        dict_sampling = {k:v for k, v in dict_sampling.iteritems() if len(v) > 2}
        for k, v in dict_sampling.iteritems():
            for i, j in v[2].iteritems():
                smpled_df = temp_df.filter(f.col(i) == 1).sampleBy('holdout_flag', fractions = {j[0][0]:j[0][1], j[1][0]:j[1][1]}, seed = 12345678)
                temp_df = temp_df.filter(f.col(i) == 0).unionAll(smpled_df)
                del smpled_df
                gc.collect()
        #Creating dictionary to update mail and control flags against each subsegment
        dict_mail_cntrl_grp = self.creatingMailControlGrpDictionary(str_holdout_flag)
        #Updating Experian Cell Code
        dict_experian_Codes = {str(r['Segment']):str(r['Experian Cell Code']) for i, r in testplan[['Segment', 'Experian Cell Code']].drop_duplicates().iterrows()}
        func_cell_code = f.udf(lambda x : dict_experian_Codes[x], t.StringType())        
        temp_df = temp_df.withColumn('experian_cell_code', func_cell_code(f.col(segname)))
        
        #Creating Mail and Control group variables for Subsegments
        return temp_df.select(["*"] +
                [
                    eval('&'.join([
                        '(temp_df["' + c + '"].isin(["'+ v + '"]))' for c, v in dict_mail_cntrl_grp[sk].iteritems()                    
                    ])).cast("int").alias(sk)
                    for sk in dict_mail_cntrl_grp.keys()
                ]
            )
        
        
class SegmentPlanSegValues():
    def __init__(self, tbl, argv_input):
        self.tbl_x = tbl
        self.cmpn_name = argv_input.split('_')[1]
        self.col_names = list(tbl.columns)
        print "\n-----------------------------------------------------Step 1 : Creating Segment Flags--------------------------------------------------------------------\n"
        
    def creatingSegments(self, dicInput, hc):
        tbl_names = {}
        ls_seg_cols = self.col_names[:]
#        self.col_names.append(self.cmpn_name + '_segment')
        for item in dicInput:
            ls_seg_cols.append(item.strip().split(' ')[0].lower()+ '_segment')
            k = pd.DataFrame({'keys' : [x.strip() for x in dicInput[item]['Logic'].keys()], 'values' : [str(x.strip()) for x in dicInput[item]['Logic'].values()]})
            for index, row in k.iterrows():
                if int(index) == 0:
                    df1 = pd.DataFrame({"temp" : str(item), str(row['keys']).strip()+'_s' : [x.strip() for x in row['values'].split(', ')]})
                else:
                    df2 = pd.DataFrame({"temp" : str(item), str(row['keys']).strip()+'_s' : [x.strip() for x in row['values'].split(', ')]})
                    fnl_df = pd.merge(df1, df2, on = ["temp"])
                    df1 = fnl_df
            fnl_df.drop('temp', axis= 1, inplace = True)
            fnl_df[item.strip().split(' ')[0].lower() + '_segment'] = item.strip()
            fnl_df['key_s'] = fnl_df[[x.strip()+'_s' for x in k['keys'].tolist()]].apply(lambda x : ''.join(x.astype(str)).strip(), axis = 1)
            tbl_names[item] = hc.createDataFrame(fnl_df)
#            tbl_names[item].show()
            intrm_tbl = self.tbl_x.withColumn('key', f.concat_ws("",*[f.col(x.strip()) for x in k['keys'].tolist()]))
            self.tbl_x = intrm_tbl.join(f.broadcast(tbl_names[item]), f.col('key') == f.col('key_s'), 'left').select(ls_seg_cols)
            print "Created {0} Segment".format(item)
        return self.generateSegmentCount(ls_seg_cols)
            
    def generateSegmentCount(self, ls_seg_cols):
        updated_xtab = self.tbl_x.fillna('').withColumn(self.cmpn_name+'_segment', f.concat_ws('',*[f.col(x) for x in list(set(self.col_names) ^ set(ls_seg_cols))]))
        self.col_names.append(self.cmpn_name+'_segment')
        print "Updated Segment Level Counts are : "
        filtered_xtab = updated_xtab.where(~f.col(self.cmpn_name+'_segment').isin('')).persist(StorageLevel.MEMORY_AND_DISK)
        filtered_xtab.crosstab(self.cmpn_name+'_segment', 'holdout_flag').show()
        
        return filtered_xtab.select(self.col_names)
        
def main(argv):
    try:
        #####------------------------------------Validating Input sheet-----------------------------------------------###
        sc = SparkContext(appName="Test Plan for WMT Direct campaign")
        assert len(argv) == 4, "Command Line arguments passed not meeting minimum criteria of passing only crossTab name, symbol for 'All' and Iteration number!"
        if int(argv[3]) == 1:
#            sc = SparkContext(appName="Test Plan for WMT Direct campaign")    
            hc = HiveContext(sc)
            pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
            obj_plan = UpdatingInputPlan(pd.read_excel("TestPlan.xlsx", sheetname = 'TestPlan', skiprows = 1), \
            pd.read_excel('TestPlan.xlsx', sheetname = 'SegmentPlan', skiprows = 1), \
            pd.read_excel('TestPlan.xlsx', sheetname = 'SubsegmentPlan'), argv[1]) #ak_Ensure_crosstab_21Jun2017
            
            hc.sql('use ckpiri')        
            query = "select * from {0}".format(argv[1])
            tbl_xtab = hc.sql(query)
            
            obj_seg = SegmentPlanSegValues(tbl_xtab, argv[1])
            fil_xtab = obj_seg.creatingSegments(obj_plan.dictSegment, hc)
            
            fil_xtab.write.mode('overwrite').saveAsTable(argv[1].split('_')[1] + '_mailctrl_Base', format = "orc")
            print "\nSaved Base Mail Control Table {0}_mailctrl_Base".format(argv[1].split('_')[1])
        
            obj_testplan = TestPlanSegValues(obj_plan.testPlan, obj_plan.dictSegment, obj_plan.dictSubSegment, argv[2], argv[1])
            fil_xtab = obj_testplan.updatingSubSegments_brules(fil_xtab, obj_plan.testPlan, obj_seg.cmpn_name+'_segment')
            
            print "\n--------------------------------------------------- All Modules Completed Successfully --------------------------------------------------------------------\n"
            
            fil_xtab.write.mode('overwrite').saveAsTable(argv[1].split('_')[1]+'_mailctrlv'+argv[3], format = "orc")
            print '\n Saved Mail Control Table for Iteration {0} as {1}_mailctrlv{0}'.format(argv[3], argv[1].split('_')[1])
            
            del obj_seg, obj_plan, obj_testplan
        
        else:
#            sc = SparkContext(appName="Test Plan for WMT Direct campaign")    
            hc = HiveContext(sc)
            pd.options.mode.chained_assignment = None #This will allow container to stop displaying pandas Warnings
            obj_plan = UpdatingInputPlan(pd.read_excel("TestPlan.xlsx", sheetname = 'TestPlan'), \
            pd.read_excel('TestPlan.xlsx', sheetname = 'SegmentPlan', skiprows = 1), \
            pd.read_excel('TestPlan.xlsx', sheetname = 'SubsegmentPlan'), argv[1])
        
            hc.sql('use ckpiri')        
            query = "select * from {0}".format(argv[1].split('_')[1]+'_mailctrl_Base')
            fil_xtab = hc.sql(query)
            
            obj_testplan = TestPlanSegValues(obj_plan.testPlan, obj_plan.dictSegment, obj_plan.dictSubSegment, argv[2], argv[1])
            fil_xtab = obj_testplan.updatingSubSegments_brules(fil_xtab, obj_plan.testPlan, argv[1].split('_')[1]+'_segment')
            
            print "\n--------------------------------------------------- All Modules Completed Successfully --------------------------------------------------------------------\n"
            
            fil_xtab.write.mode('overwrite').saveAsTable(argv[1].split('_')[1]+'_mailctrlv'+argv[3], format = "orc")
            print '\n Saved Mail Control Table for Iteration {0} as {1}_mailctrlv{0}'.format(argv[3], argv[1].split('_')[1])
            
            del obj_plan, obj_testplan
            
        gc.collect()
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