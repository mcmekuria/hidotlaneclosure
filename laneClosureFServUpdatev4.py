
# coding: utf-8


import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , timedelta
from time import  strftime
import math
from os import listdir
from arcgis.gis import GIS
from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
from cmath import isnan
from math import trunc
#import ago

try:
    import urllib.request, urllib.error, urllib.parse  # Python 2
except ImportError:
    import urllib.request as urllib2  # Python 3
import zipfile
from zipfile import ZipFile
import json
import fileinput
from os.path import isdir, isfile, join

#%matplotlib inline
import matplotlib.pyplot as pyd
from IPython.display import display #, YouTubeVideo
from IPython.display import HTML
import pandas as pd
#from pandas import DataFrame as pdf

#import geopandas as gpd

from arcgis import geometry
from arcgis import features 
import arcgis.network as network

from arcgis.features.analyze_patterns import interpolate_points
import arcgis.geocoding as geocode
from arcgis.features.find_locations import trace_downstream
from arcgis.features.use_proximity import create_buffers
from arcgis.features import GeoAccessor as gac, GeoSeriesAccessor as gsac
from arcgis.features import SpatialDataFrame as spedf

from arcgis.features import FeatureLayer

import numpy as np

from copy import deepcopy

def webexsearch(mgis, title, owner_value, item_type_value, max_items_value=1000,inoutside=False):
    item_match = None
    search_result = mgis.content.search(query= title + ' AND owner:' + owner_value, 
                                          item_type=item_type_value, max_items=max_items_value, outside_org=inoutside)
    if "Imagery Layer" in item_type_value:
        item_type_value = item_type_value.replace("Imagery Layer", "Image Service")
    elif "Layer" in item_type_value:
        item_type_value = item_type_value.replace("Layer", "Service")
    
    for item in search_result:
        if item.title == title:
            item_match = item
            break
    return item_match

def lyrsearch(lyrlist, lyrname):
    lyr_match = None
   
    for lyr in lyrlist:
        if lyr.properties.name == lyrname:
            lyr_match = lyr
            break
    return lyr_match

def create_section(lyr, hdrow, chdrows,rtefeat):
    try:
        object_id = 1
        pline = geometry.Polyline(rtefeat)
        feature = features.Feature(
            geometry=pline[0],
            attributes={
                'OBJECTID': object_id,
                'PARK_NAME': 'My Park',
                'TRL_NAME': 'Foobar Trail',
                'ELEV_FT': '5000'
            }
        )

        lyr.edit_features(adds=[feature])
        #_map.draw(point)

    except Exception as e:
        print("Couldn't create the feature. {}".format(str(e)))
        

def fldvartxt(fldnm,fldtyp,fldnull,fldPrc,fldScl,fldleng,fldalnm,fldreq):
    fld = arcpy.Field()
    fld.name = fldnm
    fld.type = fldtyp
    fld.isNullable = fldnull
    fld.precision = fldPrc
    fld.scale = fldScl
    fld.length = fldleng
    fld.aliasName = fldalnm
    fld.required = fldreq
    return fld

def df_colsame(df):
    """ returns an empty data frame with the same column names and dtypes as df """
    #df0 = pd.DataFrame.spatial({i[0]: pd.Series(dtype=i[1]) for i in df.dtypes.iteritems()}, columns=df.dtypes.index)
    return df

def offdirn(closide,dirn1):
    if closide == 'Right':
        offdirn1 = 'RIGHT'
    elif closide == 'Left':
        offdirn1 = 'LEFT'
        dirn1 = -1*dirn
    elif closide == 'Center':
        offdirn1 = 'RIGHT'
        dirn1 = 0.5
    elif closide == 'Both':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Directional':
        if dirn1 == -1:
            offdirn1 = 'LEFT'
        else:
            offdirn1 = 'RIGHT'
    elif closide == 'Full' or closide == 'All':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Shift':
        offdirn1 = 'RIGHT'
    elif closide == 'Local':
        offdirn1 = 'RIGHT'
    else:
        offdirn1 = 'RIGHT'
        dirn1 = 0 
    return offdirn1,dirn1

def deleteupdates(prjstlyrsrc, sectfeats):
    for x in prjstlyrsrc:
        print (" layer: {} ; from item : {} ; URL : {} ; Container : {} ".format(x,x.fromitem,x.url,x.container))
        if 'Projects' in (prjstlyrsrc):
            xfeats =  x.query().features
            if len(xfeats) > 0:
                if isinstance(xfeats,(list,tuple)):
                    if "OBJECTID" in xfeats[0].attributes:
                        oids = "'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xfeats if 'OBJECTID' in xfs.attributes ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats[0].attributes:    
                        oids = "'" + "','".join(str(xfs.attributes['OID']) for xfs in xfeats if 'OID' in xfs.attributes ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                elif isinstance(xfeats,spedf):
                    if "OBJECTID" in xfeats.columns:
                        oids = "'" + "','".join(str(f1.get_value('OBJECTID')) for f1 in xfeats ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats.columns:    
                        oids = "'" + "','".join(str(f1.get_value('OID')) for f1 in xfeats ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                if 'None' in oids:
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                else:
                    x.delete_features(where=oidqry)


    # get the date and time
curDate = strftime("%Y%m%d%H%M%S") 
# convert unixtime to local time zone
#x1=1547062200000
#tbeg = datetime.date.today().strftime("%A, %d. %B %Y %I:%M%p")
tbeg = datetime.date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())


### Start setting variables for local operation
#outdir = r"D:\MyFiles\HWYAP\laneclosure\Sections"
#lcoutputdir =  r"C:\\users\\mmekuria\\ArcGIS\\LCForApproval"
#lcfgdboutput = "LaneClosureForApproval.gdb" #  "Lane_Closure_Feature_WebMap.gdb" #
#lcfgdbscratch =  "LaneClosureScratch.gdb"
# output file geo db 
#lcfgdboutpath = "{}\\{}".format(lcoutputdir, lcfgdboutput)

# ArcGIS user credentials to authenticate against the portal
credentials = { 'userName' : 'dot_mmekuria', 'passWord' : '****'}
#credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
# Address of your ArcGIS portal
portal_url = r"http://histategis.maps.arcgis.com/" # r"https://www.arcgis.com/" # 

# ID or Title of the feature service to update
#featureService_ID = '9243138b20f74429b63f4bd81f59bbc9' # arcpy.GetParameter(0) #  "3fcf2749dc394f7f9ecb053771669fc4" "30614eb4dd6c4d319a05c6f82b049315" # "c507f60f298944dbbfcae3005ad56bc4"
lnclSrcFSTitle = 'LaneClosure' # arcpy.GetParameter(0) 
itypelnclsrc="Feature Service" # "Feature Layer" # "Service Definition"
lnclhdrnm = 'LaneClosure'
lnclchdnm = 'Location_repeat'

fsectWebMapTitle = 'Lane_Closure_WebMap_WFL1' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypeFS="Feature Service" # "Feature Layer" # "Service Definition"
wmlnclyrptsnm = 'Lane_Closure_Begin_and_End_Points'
wmlnclyrsectsnm = 'Lane_Closure_Sections'

hirtsTitle = 'HIDOTLRS' # arcpy.GetParameter(0) #  'e9a9bcb9fad34f8280321e946e207378'
itypelrts="Feature Service" # "Feature Layer" # "Service Definition"
wmlrtsnm = 'HIDOTLRS'
rteFCSelNm = 'rtesel'
servicename =  lnclSrcFSTitle # "Lane_Closure_WebMap" # "HI DOT Daily Lane Closures Sample New" # arcpy.GetParameter(1) # 
tempPath = sys.path[0]
userName = credentials['userName'] # arcpy.GetParameter(2) # 
passWord = credentials['passWord'] # arcpy.GetParameter(3) # "ChrisMaz!1"
arcpy.env.overwriteOutput = True
#print("Temp path : {}".format(tempPath))

print("Connecting to {}".format(portal_url))
#qgis = GIS(portal_url, userName, passWord)
qgis = GIS(profile="hisagolprof")
numfs = 1000 # number of items to query
#    sdItem = qgis.content.get(lcwebmapid)
ekOrg = False
# search for lane closure source data
print("Searching for lane closure source {} from {} item for user {} and Service Title {} on AGOL...".format(itypelnclsrc,portal_url,userName,lnclSrcFSTitle))
fslnclsrc = webexsearch(qgis, lnclSrcFSTitle, userName, itypelnclsrc,numfs,ekOrg)
#qgis.content.search(query="title:{} AND owner:{}".format(lnclSrcFSTitle, userName), item_type=itypelnclsrc,outside_org=False,max_items=numfs) #[0]
#print (" Content search result : {} ; ".format(fslnclsrc))
print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fslnclsrc.url,fslnclsrc.title,fslnclsrc.id))
lnclyrsrc = fslnclsrc.layers

# header layer
lnclhdrlyr = lyrsearch(lnclyrsrc, lnclhdrnm)
hdrsdf = lnclhdrlyr.query(as_df=True)
# child layer
lnclchdlyr = lyrsearch(lnclyrsrc, lnclchdnm)
# relationship between header and child layer
#relncldesc = arcpy.Describe(lnclhdrlyr)
#relncls = relncldesc.relationshipClassNames

#for rc in relncls:
#    print (" relationshp class : {} has {}  ; Title : {} ; Id : {} ".format(fs.url,fs.title,fs.id))

route_service_url = qgis.properties.helperServices.route.url
route_service = arcgis.network.RouteLayer(route_service_url, gis=qgis)
route_layer = arcgis.network.RouteLayer(route_service_url, gis=qgis)

# search for lane closure sections 
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypeFS,portal_url,userName,fsectWebMapTitle))

#fsect = qgis.content.search(query="title:{} AND owner:{}".format(fsectWebMapTitle, userName), item_type=itypeFS,outside_org=False,max_items=numfs) #[0]
fsectwebmap = webexsearch(qgis, fsectWebMapTitle, userName, itypeFS,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))

print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fsectwebmap.url,fsectwebmap.title,fsectwebmap.id))
wmsectlyrs = fsectwebmap.layers
wmsectlyrpts = lyrsearch(wmsectlyrs, wmlnclyrptsnm)
wmlnclyrsects = lyrsearch(wmsectlyrs, wmlnclyrsectsnm)

sectfldsall = [fd.name for fd in wmlnclyrsects.properties.fields]
# get sdf to be used for new section data insert operations
sectqry = wmlnclyrsects.query()
sectfset = sectqry.features
# delete all sections without a route number
norteqry = wmlnclyrsects.query(where="Route is null")
# prepare the object Id's with no route numbers (submitted without update privileges
if len(norteqry)>0:
    norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in norteqry )  + "')"
    resultdel = wmlnclyrsects.delete_features(where=norteid)

#sectsdf = wmlnclyrsects.query(as_df=True)
sectsdf = sectqry.sdf
# get sdf to be used for new point data insert operations
ptqry = wmsectlyrpts.query()
#ptsdf = wmsectlyrpts.query(as_df=True)
ptsfset = ptqry.features
ptsfldsall = [fd.name for fd in wmsectlyrpts.properties.fields]
ptsdf = ptqry.sdf
ptcols = ptsdf.columns
sdfptsapp = deepcopy(ptsdf.head(1))
# create an empty dataset with all the field characteristics
ptslnclsdf = deepcopy(ptsdf.head(0)) #df_colsame(sdfptsapp)
# search for Route dataset  
print("Searching for {} from {} item for user {} and Service Title {} on AGOL...".format(itypelrts,portal_url,userName,hirtsTitle))

numfs = 4000 # number of items
fswbrts = webexsearch(qgis, hirtsTitle, userName, itypelrts,numfs,ekOrg)
#print (" Content search result : {} ; ".format(fsect))

print (" Feature URL: {} ; Title : {} ; Id : {} ".format(fswbrts.url,fswbrts.title,fswbrts.id))
wmrtslyrs = fswbrts.layers
wmlyrts = lyrsearch(wmrtslyrs, wmlrtsnm)

# search radius
searchRadius = 1000

# layer names for the linear reference results
rteFCSel = "RteSelected"
rtevenTbl = "RteLinEvents"
eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
eveLRSFC = "RteLinEvtFC"
outFeatseed = "EvTbl"
lrsGeoPTbl = """LRS_{}""".format(outFeatseed) # DynaSeg result feature table created from LRS points location along routes 
outfeatbl = """Rt{}""".format(outFeatseed) 
# linear reference link properties 
eveProPts = "Route POINT BMP EMP"
eveProLines = "Route LINE BMP EMP"

# linear reference fields 
OidFld = fldvartxt("ObjectID","LONG",False,28,"","","OID",True) 
# create the bmp and direction field for the merged result table 
RteFld = fldvartxt("Route","TEXT",False,"","",60,"ROUTE",True) 
fldrte = RteFld.name
# create the bmp and direction field for the merged result table 
bmpFld = fldvartxt("BMP","DOUBLE",False,18,11,"","BMP",True) 

# create the emp and direction field for the result table 
#empFld = arcpy.Field()
empFld = fldvartxt("EMP","DOUBLE",True,18,11,"","EMP",False) 
ofFld = fldvartxt("Offset","DOUBLE",True,18,11,"","Offset",False) 


mptbl = str(arcpy.CreateTable_management("in_memory",rtevenTbl).getOutput(0))


# add BMP , EMP and RteDirn fields to the linear reference lane closure table
#arcpy.AddField_management(mptbl, OidFld.name, OidFld.type, OidFld.precision, OidFld.scale)
arcpy.AddField_management(mptbl, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
arcpy.AddField_management(mptbl, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
arcpy.AddField_management(mptbl, empFld.name, empFld.type, empFld.precision, empFld.scale)
arcpy.AddField_management(mptbl, ofFld.name, ofFld.type, ofFld.precision, ofFld.scale)


# create the milepost insert cursor fields  
mpflds = [RteFld.name,bmpFld.name,empFld.name,ofFld.name]
# create the MilePost Insert cursor 
mpinscur = arcpy.da.InsertCursor(mptbl, mpflds)  

# query the lane closure survey entries that have been updated by users and delete them from processed feature class
#print (" Section parent global id list : {} ".format(featlisthdrgid))
lnclupdatelistqry = "EditDate > CreationDate" # and EditDate>= '{}'".format((datetime.datetime.today()-timedelta(days=1)).strftime("%m/%d/%y"))
# lnclupdatelistqry = "EditDate > CreateDate and EditDate>= '{}'".format((datetime.datetime.today()-timedelta(days=1)).strftime("%m/%d/%y"))
# lane closure header features without generated sections 
lnclupdatefeats =  lnclhdrlyr.query(where=lnclupdatelistqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
# get location detail related survey repeat records for the parent record object id    
featuplnclsdf = lnclupdatefeats.sdf #[l1w for l1 in featsectgid  ]
featuplnclsdf['EdiTime'] = pd.to_datetime(featuplnclsdf.EditDate)
lfeats = lnclupdatefeats.features
#oidqry = "objectId in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in lfeats if 'objectid' in lfs.attributes and isinstance(lfeats,(list,tuple)) ) + "')"
# prepare the global Id's that have been updated using the comparison creation and update dates 
pgidqry = "parentglobalid in ('" + "','".join(str(lfs.attributes['globalid']) for lfs in lfeats if 'globalid' in lfs.attributes and isinstance(lfeats,(list,tuple)) ) + "')"
# query the section features from the section layer the records that need to be removed
featsectschg = wmlnclyrsects.query(where=pgidqry,out_fields=sectfldsall) #.sdf # + ,out_fields="globalid")
xsectchgsdf = featsectschg.sdf
xftsectschg = featsectschg.features
scols = xsectchgsdf.columns
xsectchgsdf['EdiTime'] = pd.to_datetime(xsectchgsdf.EditDate)
for srow in xsectchgsdf.itertuples():
    xsedte = srow.EdiTime # srow['EditDate']
    xsedts = datetime.datetime.timestamp(xsedte) # + datetime.timedelta(int(utcofs)))
    xsparguid = srow.parentglobalid # srow['parentglobalid']
    xsoid =  srow.OBJECTID # srow['OBJECTID']
    xlfeatsdf = featuplnclsdf[featuplnclsdf['globalid'].isin([xsparguid])]
    lcols = xlfeatsdf.columns
    #print(' Sect OID {} ; pgid : {} ; Creator : {} ; EditSectime : {} ;  Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, srow.Creator, srow.EditDate, srow.Route))
    for lrow in xlfeatsdf.itertuples():
        xledte =  lrow.EdiTime #lrow['EditDate']
        utcofs = lrow.utcoff
        if isnan(utcofs):
            utcofs = -10 
        xledts = datetime.datetime.timestamp(xledte) # - datetime.timedelta(hours=int(utcofs)))
        #xledts = datetime.datetime.timestamp(xledte)
        xlguid = lrow.globalid # getattr(lrow,'globalid') # lrow['globalid']
        xloid =  lrow.objectid #lrow['objectid']
        #print(' OID Sect {} ; pgid : {} ;  LnClgid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, lrow.globalid, srow.Creator, srow.EditDate, lrow.EditDate,lrow.Route))
        #print(' GID Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
        if round(xsedts,2) < round(xledts,2):   
            oidsectschg = "OBJECTID in ('" + str(xsoid) + "')"
            resultdel = wmlnclyrsects.delete_features(where=oidsectschg)
            print(' Deleted section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited : {} ; deleted : {} \n '.format(
                    srow.OBJECTID,lrow.Route,srow.parentglobalid , lrow.globalid,
                     srow.Creator, srow.EditDate, lrow.EditDate,resultdel))

##for xfs in xftsectschg:
##	for lfs in lfeats:
##		#print(' Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
##		if xfs.attributes['parentglobalid'] == lfs.attributes['globalid'] and xfs.attributes['EditDate']< lfs.attributes['EditDate']:
##			print(' Matching Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))


# query the point features, from the beg and end point feature layer, the records that need to be removed
featptschg = wmsectlyrpts.query(where=pgidqry,out_fields=ptsfldsall)
xftptschg = featptschg.features 
##if len(xftptschg) > 0:
##    oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftptschg if 'OBJECTID' in xfs.attributes and isinstance(xftptschg,(list,tuple)) ) + "')"
##    wmsectlyrpts.delete_features(where=oidptschg)
for xfs in xftptschg:
    for lfs in lfeats:
        if xfs.attributes['parentglobalid'] == lfs.attributes['globalid'] and xfs.attributes['EditDate']< lfs.attributes['EditDate']:
            oidptschg = "OBJECTID in ('" + str(xfs.attributes['OBJECTID']) + "')"
            #oidptschg = "OBJECTID in ('" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xftsectschg if 'OBJECTID' in xfs.attributes and isinstance(xftsectschg,(list,tuple)) ) + "')"
            resultdel = wmsectlyrpts.delete_features(where=oidptschg)
            print('Deleted Beg & End Points {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; deleted : {} '.format(
                oidptschg,datetime.datetime.fromtimestamp(lfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),
                datetime.datetime.fromtimestamp(xfs.attributes['EditDate']/1e3 , tzlocal.get_localzone()),xfs.attributes['Route'], 
                xfs.attributes['BMP'], xfs.attributes['EMP'], xfs.attributes['RteDirn'], xfs.attributes['ClosureSide'],resultdel ))

    #datetime.datetime.fromtimestamp(enDte/1e3).strftime("%m-%d-%y")

# update the lane closure feature creation date to match the edit date so they will be the same
lnclupdatefeats =  lnclhdrlyr.query(where=lnclupdatelistqry,out_fields=('*'))
#flnclcredit = ["OID : {} Creator : {}  date : {}  Editor : {}  date : {} , "
#.format(fd.attributes['objectid'],fd.attributes['Creator'],fd.attributes['CreationDate'],fd.attributes['Editor'],
#fd.attributes['EditDate']) for fd in lnclupdatefeats]

lnclchgisdf = lnclupdatefeats.sdf
lnclchgisdf.set_index('objectid',inplace=True)

#for feat in lnclupdatefeats:
#    feat.set_value ('CreationDate',feat.get_value('EditDate'))
#lnclchgfeats = arcgis.features.FeatureSet.from_dataframe(lnclchgisdf)

#flnclcredit = ["OID : {} Creator : {}  date : {}  Editor : {}  date : {} , ".format(fd.attributes['objectid'],fd.attributes['Creator'],fd.attributes['CreationDate'],fd.attributes['Editor'],fd.attributes['EditDate']) for fd in lnclchgfeats]

#lnclchgisdf.loc[lnclchgisdf['CreationDate']<lnclchgisdf['EditDate'],'CreationDate'] = lnclchgisdf.loc[0,'EditDate']

#updict = lnclhdrlyr.edit_features(None,lnclchgfeats)

# First get the id's of the sections already processed
#feat_set = wblnclyrsects.query(where="Route is not null").sdf # + ,out_fields="globalid") #
sectflds = ['parentglobalid','route','BMP','EMP']
featsects = wmlnclyrsects.query(where="Route is not null",out_fields=sectflds) #.sdf # + ,out_fields="globalid")
featsectdf = deepcopy(featsects.sdf) #[l1 for l1 in featsectgid  ]
#print (" section list : {} ".format(featsectlist))
#featlisthdrgid1 = "','".join(x[1:(len(x)-1)] for x in featsectdf.parentglobalid if ('{' and '}') in x )
featlisthdrgid2 = "','".join(x.upper() for x in featsectdf.parentglobalid) #if ('{' and '}') not in x )
featlisthdrgid = "'" + featlisthdrgid2 + "'" # + "','" + featlisthdrgid2 
#print (" Section parent global id list : {} ".format(featlisthdrgid))
featlisthdrgidqry = " globalid not in ({}) ".format(featlisthdrgid)
# lane closure header features without generated sections 
lnclhdrnewfeats =  lnclhdrlyr.query(where=featlisthdrgidqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
# get location detail related survey repeat records for the parent record object id    
lnclhdroids = "','".join(str(x) for x in hdrsdf.objectid )
hdridsqry = "{}{}{} ".format("\'",lnclhdroids,"\'")
repts = lnclhdrlyr.query_related_records(hdridsqry,"1",return_geometry=True)
reptdata = repts['relatedRecordGroups']
#["{}".format(x) for x in [ m['relatedRecords'] for m in reptdata]]

# collect lane closure Rxfs.attributes['EditDate']< lfs.attributes['EditDate']:epeat Point features without generated sections 
ptcols = ptsdf.columns
if len(ptcols)>0:
    lnclbeptspgid = "','".join(x for x in ptsdf.parentglobalid  )
    lnclbeptspgid = "{}{}{}".format("\'",lnclbeptspgid,"\'")
    lnclbeptsgid = "','".join(x for x in ptsdf.globalid )
    lnclbeptsgid = "{}{}{}".format("\'",lnclbeptsgid,"\'")
    lnclbeptspgidqry = " parentglobalid not in ({}) ".format(lnclbeptspgid)
else:
    lnclbeptspgid = "{}{}{}".format("\'","","\'")    
    lnclbeptspgidqry = " parentglobalid like '%'"
#print (" Section parent global id list : {} ".format(featlisthdrgid))
# lane closure child features without generated sections 
lnclbepts =  lnclchdlyr.query(where=lnclbeptspgidqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
if len(lnclhdrnewfeats)>0:
    # feature class parameters  
    has_m = "DISABLED"
    has_z = "DISABLED"
    geotype = "POINT"
    spref = lnclhdrnewfeats.spatial_reference
    
    # geopoint featureclass to store the lane closure locations 
    geoPtFC = arcpy.CreateFeatureclass_management("in_memory", lrsGeoPTbl, geotype,spatial_reference=arcpy.SpatialReference(spref['wkid']))#,'',has_m, has_z,spref) #.getOutput(0))
    
    # add BMP , EMP and RteDirn fields to the linear reference lane closure table
    arcpy.AddField_management(geoPtFC, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
    arcpy.AddField_management(geoPtFC, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
    arcpy.AddField_management(geoPtFC, empFld.name, empFld.type, empFld.precision, empFld.scale)
    
    geoPtFCnm = "{}".format(geoPtFC)
    
    ptflds = [RteFld.name,"SHAPE@XY"]
    
    ptinscur = arcpy.da.InsertCursor(geoPtFC,ptflds)
    
    # get the list of parent features
    lnclhdrfsel = lnclhdrnewfeats.features
    lnclhdrgisdf = lnclhdrnewfeats.sdf
    # Header fields ['ApprLevel1', 'ApprLevel2','ApproverL1','l1email', 'ApproverL2','l2email', 'Clength', 'ClosHours', 'ClosReason','ClosType', 'CloseFact', 'ClosureSide',
    # 'CreationDate', 'Creator','DEPHWY', 'DIRPInfo', 'DistEngr', 'EditDate', 'Editor', 'Island', 'LocMode', 'NumLanes',
    #'ProjectId', 'Remarks', 'RoadName', 'Route', 'SHAPE', 'beginDate', 'enDate', 'globalid', 'objectid' ]
    
    chgflds = {"CreationDate" : "CreateDate" ,'ClosureSide' : 'ClosedSide'}
    lnclhdrgisdf.rename(columns=chgflds, inplace=True)
    #[(f.attributes['globalid'] + ' | ' + f.attributes['Route']) for f in lnclhdrnewfeats.features]
    fsnewhdrgid = [f.attributes['globalid'] for f in lnclhdrfsel]
    
    
    featlistchdgid = "','".join(x for x in fsnewhdrgid) 
    print (" new features for repeat list selection : {}{}{} ".format(" '",featlistchdgid,"' "))
    
    # setup the query to bring in the related child records
    featlistchdgidqry = " parentglobalid in ({}{}{}) ".format(" '",featlistchdgid,"' ")
    # lane closure detail features without generated sections 
    lnclchdnewfeats =  lnclchdlyr.query(where=featlistchdgidqry,out_fields=('*')) #.sdf # + ,out_fields="globalid")
    # get the list of child features
    lnclchdfsel = lnclchdnewfeats.features
    
    lnclistchdgisdf = lnclchdnewfeats.sdf
    # shorten the column names that are greater than 10 characters
    chgflds = {"parentglobalid" : "paglobalid", "CreationDate" : "CreateDate" }
    lnclistchdgisdf.rename(columns=chgflds,inplace=True)
    
    #[arcpy.AlterField_management(lnclchdnewfeats, f, chgflds[f]) for f in chgflds]
    
    #lnclistchdfc = lnclchdnewfeats.save("in-memory","newchdgidmemfc",encoding="utf-8") # (could not save field names longer than 10char
    fsnewchdgid = [f.attributes['globalid'] for f in lnclchdfsel]
    
    numlanes = 0
    fhdrteid = ''
    #fchdrmem = arcpy.CreateFeatureclass_management("in_memory", "fchdr", "Point",spatial_reference=arcpy.SpatialReference(spref['wkid']))
    # loop over the features that are not in the sections layer 
    for fhdr in lnclhdrnewfeats:
        objid = fhdr.attributes['objectid']
        fhdrgid = fhdr.attributes['globalid']
        loctype = fhdr.attributes['LocMode']
        clostype = fhdr.attributes['ClosType']
        closedfact = fhdr.attributes['CloseFact']
        cloSide = fhdr.attributes['ClosureSide']
        closHrs = fhdr.attributes['ClosHours']
        begDte = fhdr.attributes['beginDate']
        enDte = fhdr.attributes['enDate']
        begdt = datetime.datetime.fromtimestamp(begDte/1e3)
        endt = datetime.datetime.fromtimestamp(enDte/1e3)
        begdtl = datetime.datetime.fromtimestamp(begDte/1e3, tzlocal.get_localzone())
        endtl = datetime.datetime.fromtimestamp(enDte/1e3, tzlocal.get_localzone())
        lanes = fhdr.attributes['NumLanes']
        approverl1 = fhdr.attributes['ApproverL1']
        apprlevel1 = fhdr.attributes['ApprLevel1']
        approverl2 = fhdr.attributes['ApproverL2']
        apprlevel2 = fhdr.attributes['ApprLevel2']
        dirpinfo = fhdr.attributes['DIRPInfo']
        if lanes is None:
            lanes = 1
        if closedfact == 'Shoulder':     
            lanes = 0 #fhdr.attributes['NumLanes']
        numlanes = lanes
        remarks = fhdr.attributes['Remarks']
        rdname = fhdr.attributes['RoadName']
        creator = fhdr.attributes['Creator']
        creatime = fhdr.attributes['CreationDate']
        createdatetime = datetime.datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone())
        rteid = fhdr.attributes[RteFld.name]
    
        try:
            # get location detail related survey repeat records for the parent record object id    
            #chdrecs = lnclhdrlyr.query_related_records(fhdr.attributes['objectid'],"1",return_geometry=True)
            #chddata = chdrecs['relatedRecordGroups']
            # query the lane closure data that does not have a section
            lnclchdnewfc = lnclchdlyr.query(where = " parentglobalid in ({}{}{}) ".format(" '",fhdrgid,"' "),out_fields='*') #.sdf # + ,out_fields="globalid")
            featlnclchd = lnclchdnewfc.features
            lnclchdgisdf = lnclchdnewfc.sdf
            #lnclchdgisdf.spatial.to_featureclass('in_memory/testfc',overwrite=True)
            chgflds = {"parentglobalid" : "paglobalid", "CreationDate" : "CreateDate" }
            lnclchdgisdf.rename(columns=chgflds,inplace=True)
            chdcols = lnclchdgisdf.columns
            # merge the header data into the related dataset for further processing
            lnclfpts = lnclchdgisdf.merge(lnclhdrgisdf,how='inner',left_on='paglobalid',right_on='globalid',suffixes=('_c', '_h'))
            lnclfptscols = lnclfpts.columns
            # query the route feature for this closure
#            if fhdrteid != fhdr.attributes[RteFld.name]:
            fhdrteid = fhdr.attributes[RteFld.name]
            featlnclrte = wmlyrts.query(where = "Route in  ({}{}{}) ".format(" '",fhdrteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
            rteFCSel = featlnclrte.save('in_memory','rtesel')
            ftlnclrte = featlnclrte.features
            if (len(ftlnclrte)>0):
                rtegeo = ftlnclrte[0].geometry
                geomrte = arcgis.geometry.Geometry(rtegeo)
                rtepaths = rtegeo['paths']
                bmprte = round(rtepaths[0][0][2],3)
                emprte = round(rtepaths[0][len(rtepaths[0])-1][2],3)
                #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
                lrte = os.path.join('in_memory','rteselyr')
                arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
            #lrterows = arcpy.da.SearchCursor(lrte, [f.name for f in arcpy.ListFields(lrte)]) # all fields in rte
            #rtelyr = "rteselyr" #os.path.join('in_memory','rteselyr')
            flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
            lrterows = arcpy.da.SearchCursor(lrte,flds)
            #[print('{}, {}, {}'.format(row[0], row[2],row[1])) for row in lrterows]
            lnclfpts0= lnclfpts[['paglobalid','globalid_c','SHAPE_c','MileMarker']].tail(1).set_index('paglobalid')
            # merge the two milepost/geometry values to a single record using the parentglobalid to allow for generation of a section
            lnclfpts1 = lnclfpts.join(lnclfpts0,rsuffix='e',on='paglobalid').head(1)
            # get the data as a numpy array to convert it to an ArcGIS table
            lncldata = np.array(np.rec.fromrecords(lnclfpts1.values))
            # get the names to a list and apply them back to the data array
            lnclnm = lnclfpts1.dtypes.index.tolist()
            lncldata.dtype.names = tuple(lnclnm)
            # get each point feature geometry
            rteMP = []
            stops = ""
            #print (" a {}  ".format(" test "))
            dirn = 1
            if loctype == 'MilePost':
                # delete previous route and mile post record if it exists
                if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                    arcpy.DeleteRows_management(mptbl)
                   
                bmpval =  float(lnclfpts1['MileMarker'].values[0])
                empval = float(lnclfpts1['MileMarkere'].values[0])
                if bmpval > empval:
                    empval = bmpval 
                    bmpval = float(lnclfpts1['MileMarkere'].values[0])
                    dirn = -1
                else:
                    if empval == bmpval:
                        empval = bmpval + 0.005 # lnclfpts1['MileMarkere'].values[0]
                    elif math.isnan(bmpval) and not math.isnan(empval): 
                        bmpval = empval - 0.005   
                    elif math.isnan(empval) and not math.isnan(bmpval): 
                        empval = bmpval + 0.005  
    
                dirnlbl,dirn1 = offdirn(cloSide,dirn)
                        
                #rtemprow = [(fhdrteid, bmpval, empval)]
                #mpinscur.insertRow(rtemprow)
                if numlanes >= 1:
                    for k in range(numlanes):
                        offset = dirn1*(6+12*(k-1))
                        mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                elif closedfact == 'Shoulder':     
                    if dirnlbl == 'LEFT': 
                        offset = dirn1*-18
                    else:
                        offset = dirn1 * 18    
                    mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                else:    
                    offset =  dirn1*6
                    mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                    #mpinscur.insertRow((fhdrteid, bmpval,empval))
                mpRows = arcpy.da.SearchCursor(mptbl, mpflds)
                # create the section from the mile post data
                arcpy.MakeRouteEventLayer_lr (lrte,fldrte, mptbl, eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl) # , "#", "ERROR_FIELD")
                
            elif loctype == 'MaPoints':
                # lnclfpts1.SHAPE_x & lnclfpts1.SHAPE_xe
                # create a feature class with the two data points to generate the section
                if int(arcpy.GetCount_management(geoPtFC).getOutput(0)) > 0:
                    arcpy.DeleteRows_management(geoPtFC)
                
                for fs in featlnclchd:
                    pt1 = fs.geometry # ['SHAPE_c']
                    inrow = [fhdrteid,(pt1['x'],pt1['y'])]
                    ptinscur.insertRow(inrow)
                mpRows = arcpy.da.SearchCursor(mptbl, mpflds)  
                
                # use the geopoint data to section the route
                gevTbl = os.path.join("in_memory","lnclptbl")
                arcpy.LocateFeaturesAlongRoutes_lr(geoPtFC,lrte,fldrte,searchRadius,gevTbl,eveProPts)
                # use LRS routines to generate section
    
                if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                    arcpy.DeleteRows_management(mptbl)
                mpflds = [f.name for f in arcpy.ListFields(gevTbl)]    
                mpRows = arcpy.da.SearchCursor(gevTbl,mpflds )
                mpdata = []
                mp0 = -999
                offset = 0
                for mprow in mpRows:
                    if mp0!=mprow[1]:
                        mpdata.append(mprow[1])
                        mp0 = mprow[1]
                    
                dirn = 1 # assume positive direction
                if len(mpdata)>1:                  
                    if mpdata[1] > mpdata[0]:
                        bmpval = mpdata[0]
                        empval = mpdata[1]
                    elif mpdata[1] < mpdata[0]:
                        bmpval = mpdata[1]
                        empval = mpdata[0]
                        dirn = -1
                    else: # same point entered twice increase the mileage by 0.005 to draw line
                        bmpval = round(mpdata[0],3)
                        empval = bmpval+0.005
                elif len(mpdata)==1: # if one location is entered then use that location only
                    bmpval = round(mpdata[0],3)
                    empval = bmpval +0.005
                elif len(mpdata)==0: # if no matching route location is found (outside the 1000 units search location)  place the loction at the beginnning of the point
                    bmpval = round(bmprte,3)
                    empval = bmpval+0.005
                    mpdata.append(bmpval)
                    mpdata.append(empval)
                     
                if empval > emprte:
                    empval = emprte
                if math.trunc(bmpval*1000) == math.trunc(empval*1000):
                    empval = round(bmpval,4) + 0.005
                elif math.trunc(bmpval*1000) > math.trunc(empval*1000):
                    bmpval = round(empval,4) - 0.005
                    dirn = -1
                    
                dirnlbl,dirn1 = offdirn(cloSide,dirn)
                
                    
    
                if len(mpdata)>= 1:
                #arcpy.LocateFeaturesAlongRoutes_lr(featlnclchd,featlnclrte,"Route",500,"LRSPts","Route LINE BMP EMP")        
                    if numlanes > 1:
                        for k in range(numlanes):
                            offset = dirn1*(6+12*(k-1))
                            mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                    elif closedfact == 'Shoulder':     
                        if dirnlbl == 'LEFT': 
                            offset = dirn1*-18
                        else:
                            offset = dirn1 * 18    
                        mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
                    else:                
                        offset = dirn1*6*1
                        mpinscur.insertRow((fhdrteid, bmpval,empval,offset))
    
                    arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        ##        elif len(mpdata) == 1:
        ##            arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProPts, eveLinlyr)
                
            # get the geoemtry from the result layer and append to the section feature class
            cntLyr = arcpy.GetCount_management(eveLinlyr)
            if cntLyr.outputCount > 0:
                #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
                
                # dynamic segementaiton result layer fields used to create the closure segment  
                lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
                evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
                for srow in evelincur:
                    #insecgeo = srow.getValue("SHAPE@")
                    rtenum = srow[1]
                    insecgeo = srow[4]
                    #print("Route : {} ; Geo : {}".format(rtenum, insecgeo))
                    # construct the feature to insert using the merged dataframe and the generated LR shape
                    #section fields ['OBJECTID', 'Route', 'NumLanes', 'Creator', 'RoadName', 'ClosReason', 'EditDate', 'ProjectId',
                    #'Editor', 'Clength', 'enDate', 'ClosHours', 'ClosType', 'Remarks', 'DistEngr', 'beginDate', 'CloseFact',
                    #'globalid', 'CreationDate', 'DEPHWY', 'DIRPInfo', 'LocMode', 'ApprLevel1', 'ClosureSide', 'Island',
                    #'ApprLevel2', 'BMP', 'EMP', 'RteDirn', 'parentglobalid', 'Shape__Length']
                    # dataframe fields ['CreateDate_c', 'Creator_c', 'EditDate_c', 'Editor_c', 'InteRoad', 'MileMarker', 'SHAPE_c',
                    # 'clos_pt_id', 'globalid_c', 'objectid_c', 'paglobalid', 'ApprLevel1', 'ApprLevel2', 'Clength', 'ClosHours',
                    # 'ClosReason', 'ClosType', 'CloseFact', 'ClosedSide', 'CreateDate_h', 'Creator_h', 'DEPHWY', 'DIRPInfo',
                    # 'DistEngr', 'EditDate_h', 'Editor_h', 'Island', 'LocMode', 'NumLanes', 'ProjectId', 'Remarks', 'RoadName',
                    # 'Route', 'SHAPE_h', 'beginDate', 'enDate', 'globalid_h', 'objectid_h','globalid_ce', 'SHAPE_ce', 'MileMarkere']
                    sdfsectapp = deepcopy(sectsdf.head(1))
                    #[ sdfsectapp[f1] for f1 in sdfsectapp]
                    
                    # update the values for the sdf to match the new section using the merged dataset
                    # sync dataframe merged header/detail laneclosure merge column names to match the section data
                    chgflds = {"paglobalid" : "parentglobalid" , "CreateDate_h" : "CreationDate",'Creator_h' : 'Creator',
                               'EditDate_h' : 'EditDate', 'Editor_h' : 'Editor', 'globalid_h' : 'globalid',
                               'MileMarker' : 'BMP', 'MileMarkere' : 'EMP'}
                    lnclfpts2 = deepcopy(lnclfpts1)
                    lnclfpts2.rename(columns=chgflds,inplace=True)
                    #print (" b {}  ".format(" test "))
                    # update the parentglobalid field to upper-case and inside braces
                    if ('{' and '}') not in sdfsectapp.loc[0,'parentglobalid']:
                        sdfsectapp.loc[0,'parentglobalid'] = "{}{}{}".format("{",(sdfsectapp.loc[0,'parentglobalid']).upper(),"}")
                        #print (" c {}  ".format(" test "))
                    else:
                        sdfsectapp.loc[0,'parentglobalid'] = "{}".format((sdfsectapp.loc[0,'parentglobalid']).upper())
                        #print (" d {}  ".format(" test "))
                    #update columns with matching field names
                    sdfsectapp.loc[0,'BMP'] = bmpval
                    sdfsectapp.loc[0,'EMP'] = empval
                    sdfsectapp.loc[0,'RteDirn'] = dirn
                    sdfsectapp.loc[0,'ClosureSide'] = cloSide
                    #createdate = datetime.datetime.fromtimestamp(sdfsectapp.loc[0,'CreationDate'].timestamp() , tzlocal.get_localzone())
                    #sdfsectapp.loc[0,'CreationDate'] = datetime.datetime.fromtimestamp(sdfsectapp.loc[0,'CreationDate'].timestamp() , tzlocal.get_localzone())
                    #sdfsectapp.loc[0,'EditDate'] = datetime.datetime.fromtimestamp(sdfsectapp.loc[0,'EditDate'].timestamp() , tzlocal.get_localzone())
                    
                    #print (" f {}  ".format(" test "))
                    sdfsectapp.update(lnclfpts2)
                    sdfsectapp.loc[0,'ApprLevel1'] = 'Pending'
                    sdfsectapp.loc[0,'ApprLevel2'] = 'Pending'
                    sdfsectapp.loc[0,'ApproverL1'] = approverl1
                    sdfsectapp.loc[0,'ApproverL2'] = approverl2
                    sdfsectapp.loc[0,'Active'] = '1'
                    if dirn in (0,1,-1):
                        # update the geometry
                        if insecgeo == None:
                            print('Not able to create spatial data for entry by User {} ; entered on {} ; Route {} ; BMP : {} ; EMP : {} ; Direction : {}; {} side ; Location : {} ; Period : {} ; Begin {} & End ;  No section is created.'.format(creator,createdatetime,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],locmode,closHrs,begdt,endt ))
                        else:
                            sdfsectapp.loc[0,'SHAPE'] = insecgeo
                            #sdfsectapp.set_geometry(insecgeo)
                            if closHrs is None:
                                for dt in range(0, int((endt-begdt).days)+1,1):
                                    begdti = begdt+ datetime.timedelta(days=dt)
                                    endti =  endt- datetime.timedelta(int((endt-begdt).days) - dt)
                                    sdfsectapp.loc[0,'beginDate']= begdti # begdt + datetime.timedelta(days=dt)
                                    sdfsectapp.loc[0,'enDate'] = endti #endt - datetime.timedelta(int((endt-begdt).days) - dt)
                                    newlnclfs = sdfsectapp.spatial.to_featureset()
                                    wmlnclyrsects.edit_features(newlnclfs)
                                    print('Entry by {} on {} Section Closure of Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created'.format(creator,createdatetime,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                            else:   # closHrs: is not None?  #('Other' in sdfsectapp.loc[0,'ClosHours']):

                                if '24Hrs' in closHrs: 
                                    newlnclfs = sdfsectapp.spatial.to_featureset()
                                    wmlnclyrsects.edit_features(newlnclfs)
                                    print('Entry by {} on {} Section {} Closure of Route {} ; BMP : {} ; EMP : {} ; Direction : {}; {} side ; beg {} & end {} created'.format(creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdt,endt ))
                                    
                                elif 'Daily' in closHrs or 'Other' in closHrs:  #('Other' in sdfsectapp.loc[0,'ClosHours']):
                                    for dt in range(0, int((endt-begdt).days)+1,1):
                                        begdti = begdt+ datetime.timedelta(days=dt)
                                        endti =  endt- datetime.timedelta(int((endt-begdt).days) - dt)
                                        sdfsectapp.loc[0,'beginDate']=begdti #+ datetime.timedelta(days=dt)
                                        sdfsectapp.loc[0,'enDate'] = endti #- datetime.timedelta(int((endt-begdt).days) - dt)
                                        newlnclfs = sdfsectapp.spatial.to_featureset()
                                        wmlnclyrsects.edit_features(newlnclfs)
                                        print('Entry by {} on {} Section {} Closure of Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created'.format(creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                                elif 'Overnight' in closHrs:  #('Other' in sdfsectapp.loc[0,'ClosHours']):
                                    for dt in range(0, int((endt-begdt).days)+1,1):
                                        begdti = begdt+ datetime.timedelta(days=dt)
                                        endti =  endt- datetime.timedelta(int((endt-begdt).days) - dt)
                                        sdfsectapp.loc[0,'beginDate']= begdti # + datetime.timedelta(days=dt)
                                        sdfsectapp.loc[0,'enDate'] = endti # - datetime.timedelta(int((endt-begdt).days) - dt)
                                        newlnclfs = sdfsectapp.spatial.to_featureset()
                                        wmlnclyrsects.edit_features(newlnclfs)
                                        print('Entry by {} on {} Section {} Closure of Rt {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side and beg {} & end {} created'.format(creator,createdatetime,closHrs,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'],begdti,endti ))
                            #print('{} Closure Rte {} ; BMP : {} ; EMP : {} ; Direction : {} ; {}  side ; pgid {} section created'.format(closHrs,sdfsectapp.loc[0,'Route'], sdfsectapp.loc[0,'BMP'], sdfsectapp.loc[0,'EMP'], sdfsectapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'ClosureSide'], sdfsectapp.loc[0,'parentglobalid'] ))
                        # insert the point into the point table
                        # insert the closure entry points into the points layer with the same credentials
                        geoPt = {}
                        #for f in lnclchdgisdf.itertuples():
                        for fchd in featlnclchd:
                            chdgeo = fchd.geometry
                            #print (" g {}  ".format(" test "))
                            ptgid = fchd.get_value('parentglobalid')
                            if ptgid not in lnclbeptspgid:  # point has already been entered
                                geomchd= arcgis.geometry.Geometry(chdgeo).project_as(3857)
                                sdfptsapp.update(lnclfpts2)
                                if geoPt!=geomchd:  # check if coordinates are the same and if so skip 
                                    sdfptsapp.iat[0,sdfptsapp.columns.get_loc('SHAPE')]=geomchd
                                    sdfptsapp.iat[0,sdfptsapp.columns.get_loc('BMP')] = bmpval #fchd.get_value('MileMarker')
                                    sdfptsapp.iat[0,sdfptsapp.columns.get_loc('EMP')] = empval # fchd.get_value('MileMarker')
                                    #sdfptsapp.iat[0,sdfptsapp.columns.get_loc('InteRoad')] = fchd.get_value('InteRoad')
                                    #print("{}".format(fchd.get_value('SHAPE')))
                                    try:
                                        chdlnclpts = sdfptsapp.spatial.to_featureset()
                                        wmsectlyrpts.edit_features(chdlnclpts)
                                        print('Point for Route {} ; BMP : {} ; EMP : {} ; Direction : {} , pgid {}  created'.format(sdfptsapp.loc[0,'Route'], sdfptsapp.loc[0,'BMP'], sdfptsapp.loc[0,'EMP'], sdfptsapp.loc[0,'RteDirn'], sdfsectapp.loc[0,'parentglobalid'] ))
                                    except Exception:
                                        print (" Point : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ; loc mode : {} ; with remarks {} has failed to generate sections ".format
                                               (creator,datetime.datetime.fromtimestamp(creatime/1e3 , tzlocal.get_localzone()),ptgid,objid,rteid,rdname,begDte,enDte,geomchd,remarks))
                                        
                                geoPt = geomchd
                        #chdlnclpts = ptslnclsdf.spatial.to_featureset()
                        #wmsectlyrpts.edit_features(chdlnclpts)
        
                del evelincur     
    
        except Exception:
             print (" Survey Created by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} has failed to generate sections ".format
                    (creator,createdatetime,fhdrgid,objid,rteid,rdname,begdt,endt,loctype,remarks))
            
          
    
    del ptinscur,mpinscur   

print (" End lane closure processing of {} new features ".format (len(lnclhdrnewfeats)))
