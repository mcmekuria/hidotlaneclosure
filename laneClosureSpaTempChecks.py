
# coding: utf-8


import arcpy, os , shutil, sys
import xml.dom.minidom as DOM
import arcgis
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , datetime, timedelta
from time import  strftime
import math
from os import listdir
from arcgis.gis import GIS
from arcgis.geoenrichment import *
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

import logging
logger = logging.getLogger('lnclogfile')
lnclhdlr = logging.FileHandler('lnclogfile.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
lnclhdlr.setFormatter(formatter)
logger.addHandler(lnclhdlr)
logger.setLevel(logging.INFO)

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
tbeg = date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
#tlocal = datetime.datetime.fromtimestamp(x1/1e3 , tzlocal.get_localzone())

logger.info("Spatial Conflict Processing begins at {} ".format(tbeg))

### Start setting variables for local operation
#outdir = r"D:\MyFiles\HWYAP\laneclosure\Sections"
#lcoutputdir =  r"C:\\users\\mmekuria\\ArcGIS\\LCForApproval"
#lcfgdboutput = "LaneClosureForApproval.gdb" #  "Lane_Closure_Feature_WebMap.gdb" #
#lcfgdbscratch =  "LaneClosureScratch.gdb"
# output file geo db 
#lcfgdboutpath = "{}\\{}".format(lcoutputdir, lcfgdboutput)

# ArcGIS user credentials to authenticate against the portal
#credentials = { 'userName' : 'dot_mmekuria', 'passWord' : 'xxxxxxxxx'}
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
userName = "dot_mmekuria" # credentials['userName'] # arcpy.GetParameter(2) # 
#passWord = credentials['passWord'] # arcpy.GetParameter(3) # "ChrisMaz!1"
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

#route_service_url = qgis.properties.helperServices.route.url
#route_service = arcgis.network.RouteLayer(route_service_url, gis=qgis)
#route_layer = arcgis.network.RouteLayer(route_service_url, gis=qgis)

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
dt1 = 1
td = datetime.combine(date.today(),datetime.min.time())
dts = datetime.timestamp(td) 
begdts = datetime.fromtimestamp(int(round(dts,0)))
begdt = begdts - timedelta(days=dt1)  # /1e3 , tzlocal.get_localzone()),
dt2 = 7
endt = begdt + timedelta(days=dt2)  # /1e3 , tzlocal.get_localzone()),
lncldtes = "(beginDate>= '{}')".format(begdt) # and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( begdt,endt,begdt,endt)

sectqry = wmlnclyrsects.query(where=lncldtes)
spref = sectqry.spatial_reference
budist = 500
bunit = "Feet"
busr = spref
# prepare the object Id's with no route numbers (submitted without update privileges
#if len(sectqry)>0:
#    norteid = "OBJECTID in ('" + "','".join(str(sfs.attributes['OBJECTID']) for sfs in sectqry )  + "')"
#    norteid = "objectid in ('" + "','".join(str(lfs.attributes['objectid']) for lfs in sectqry )  + "')"
    # edit the selected records
#    for sfs in  sectqry:
#        appr1 = sfs.get_value('ApproverL1') #,'dot_achung')
#        email1 = sfs.get_value('l1email') # ,'albert.chung@hawaii.gov')

# query the lane closure survey entries that have been updated by users and delete them from processed feature class
lnclsectsdf = sectqry.sdf
xftsect = sectqry.features
scols = lnclsectsdf.columns
lnclsectsdf['EdiTime'] = pd.to_datetime(lnclsectsdf.EditDate)
for srow in lnclsectsdf.itertuples():
    xsedte = srow.EditDate # srow.EdiTime # srow['EditDate']
    if pd.isnull(xsedte):
        xsedte = datetime.today()
    xsedts = datetime.timestamp(xsedte) # + datetime.timedelta(int(utcofs)))
       
    xsedtsl = datetime.fromtimestamp(xsedts , tzlocal.get_localzone())
    xbegdt = srow.beginDate # srow['beginDate']
    xbegdts = datetime.timestamp(xbegdt) # + datetime.timedelta(int(utcofs)))
    xbegdte = datetime.fromtimestamp(xbegdts) # + datetime.timedelta(int(utcofs)))
    xendt = srow.enDate # srow['enDate']
    xendts = datetime.timestamp(xendt) # + datetime.timedelta(int(utcofs)))
    xendte = datetime.fromtimestamp(xendts)
    xbegdtel = datetime.fromtimestamp(xbegdts, tzlocal.get_localzone())
    xendtel = datetime.fromtimestamp(xendts, tzlocal.get_localzone())
    xsparguid = srow.parentglobalid # srow['parentglobalid']
    xsoid =  srow.OBJECTID # srow['OBJECTID']
    remarks = srow.Remarks
    rteid = srow.Route
    rdname = srow.RoadName
    creator = srow.Creator
    creatime = srow.CreationDate
    if pd.isnull(creatime):
        creatime = datetime.today()
    creats = datetime.timestamp(creatime) # + datetime.timedelta(int(utcofs)))
    createdatetime = datetime.fromtimestamp(creats , tzlocal.get_localzone())
    loctype = srow.LocMode
    clostype = srow.ClosType
    closedfact = srow.CloseFact
    cloSide = srow.ClosureSide
    closHrs = srow.ClosHours
    begDte = srow.beginDate

#    dtefilt = ((lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt))
    #xlfeatsdf = lnclsectsdf[(lnclsectsdf.beginDate>=xbegdt and lnclsectsdf.beginDate<=xendt) or (lnclsectsdf.enDate>=xbegdt and lnclsectsdf.enDate<=xendt)]
    dteqry = "(beginDate>= '{}' and beginDate<= '{}') or (enDate>= '{}' and enDate<= '{}')".format( xbegdt,xendt,xbegdt,xendt)
    qrysectdte = wmlnclyrsects.query(where=dteqry)
    qrysectdtesdf = qrysectdte.sdf
    lcols = qrysectdtesdf.columns
    #print(' Sect OID {} ; pgid : {} ; Creator : {} ; EditSectime : {} ;  Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, srow.Creator, srow.EditDate, srow.Route))
    # check the section for time and space conflicts
    xspolygeom = srow.SHAPE
    geompart = arcgis.geometry.Geometry(xspolygeom)
#    xspolybuf = arcgis.geometry.buffer(xspolygeom, spref, budist, bunit, out_sr=spref, buffer_sr=spref, union_results=True, geodesic=True, gis=None)
    #xslconsdf = featuplnclsdf[featuplnclsdf['globalid'].isin([xsparguid])]
    for lrow in qrysectdte:
        xlguid = lrow.attributes['globalid'] # getattr(lrow,'globalid') # lrow['globalid']
        xlparguid = lrow.attributes['parentglobalid']
        if xlparguid != xsparguid:
            try:
                xledte =  lrow.attributes['EditDate']
                if pd.isnull(xledte):
                    xledte = datetime.timestamp(datetime.today())
                else:
                    xledte = xledte / 1e3    
                lspolygeom = lrow.attributes['SHAPE']
                utcofs = -10 
                xledts = datetime.fromtimestamp(xledte) # - datetime.timedelta(hours=int(utcofs)))
                xledtsl = datetime.fromtimestamp(xledte, tzlocal.get_localzone())
                geomdis = lspolygeom.disjoint(xspolygeom)
                #xledts = datetime.datetime.timestamp(xledte)
                xloid =  lrow.attributes['OBJECTID'] #lrow['objectid']
                lrte = lrow.attributes['Route']
                lpgid = lrow.attributes['parentglobalid']
                #print(' OID Sect {} ; pgid : {} ;  LnClgid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(srow.OBJECTID,srow.parentglobalid, lrow.globalid, srow.Creator, srow.EditDate, lrow.EditDate,lrow.Route))
                #print(' GID Qry {} ; pgid : {} ;  gid : {} ; LnClCreator : {} ; EditSectime : {} ; EditLnCltime : {} ; Rte : {}  '.format(xfs.attributes['OBJECTID'],xfs.attributes['parentglobalid'] , lfs.attributes['globalid'], xfs.attributes['Creator'], xfs.attributes['EditDate'], lfs.attributes['EditDate'],lfs.attributes['Route']))
                if not geomdis:   
                    oidconflicts = "OBJECTID in ('" + str(xsoid) + "')"
                    lrow.attributes['stconflict'] = '1'
                    resultupd1= wmlnclyrsects.edit_features(updates=[lrow])
                    sfeat = wmlnclyrsects.query(where=oidconflicts)
                    feats = sfeat.features
                    feat0 = feats[0]
                    updfeatval = feat0.set_value('stconflict','1')
                    #featdict = feat0.as_dict() 
                    #fset = sfeat.from_dict(featdict)
                    resultupd2 = wmlnclyrsects.edit_features(updates=[feat0])
                    print('Overlap Detected with object {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; detected : {} '.format(
                        oidconflicts,xledtsl,xsedtsl,srow.Route,srow.BMP, srow.EMP, srow.RteDirn, srow.ClosureSide,resultupd1 ))
                    print('Overlap section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited : {} ; detected : {} \n '.format(
                            srow.OBJECTID,lrte,srow.parentglobalid , lpgid,srow.Creator, srow.EditDate, xledte,resultupd2))
                    logger.info('Overlap Detected with object {} edited on {} ; date {} ; Rte : {} ; BMP : {} ; EMP : {} ; Direction : {} ; {} side ; detected : {} '.format(
                        oidconflicts,xledtsl,xsedtsl,srow.Route,srow.BMP, srow.EMP, srow.RteDirn, srow.ClosureSide,resultupd1))
                    logger.info('Overlap section {} ; Route : {}; pgid : {} ;  gid : {} ; Creator : {} ; Created : {} ; edited : {} ; detected : {} \n '.format(
                            srow.OBJECTID,lrte,srow.parentglobalid , lpgid,srow.Creator, srow.EditDate, xledte,resultupd2))
    
            except Exception as e:
                 print (" Error message : {} \n for survey Created by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} sections failed to update. ".format
                        (str(e),creator,createdatetime,xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype,remarks))
                 logger.error(" Error message : {} \n for survey Created by : {} ; date : {} ; gid : {} ; oid : {} ; Rte : {} ({}) ;  Beg Date : {}  ; End date : {} ; loc mode : {} ;  remarks {} sections failed to update. ".format
                        (str(e),creator,createdatetime,xsparguid,xsoid,rteid,rdname,xbegdts,xendts,loctype,remarks))
            
          
tend = date.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
logger.info (" End lane closure processing of {} section features. ".format (len(xftsect),tend))

print (" End lane closure processing of {} section features ended at {}. ".format (len(xftsect),tend))
