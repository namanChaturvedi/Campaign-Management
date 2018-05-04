# -*- coding: utf-8 -*-
"""
Created on Tue Mar 07 15:28:31 2017

@author: vn0bz25
"""
from enum import Enum

#Static Objects to work as enums can be kept here
class Variables(Enum):
    sum = "sum"
    subcat = "subcat"
    combo = "combo"
    loyalty = "loyalty"

class Inputs(Enum): 
    FlagName = "FlagName"
    Operation = "Operation"
    TimePeriod = "TimePeriod"
    FilterClause = "FilterClause"
    EndDate = "EndDate"
    Values = "Values"

#class CampaignData(Enum):
#    xTabName = None #This is command line to argument to pass XTab name



               