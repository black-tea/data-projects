#import arcpy
#from arcpy import env

##### Input #####
#shp = "Z:/GIS/DataLibrary/Transportation/BOE_Centerline_Intersections150930/duplicate_trial/StreetIntersections2.shp" #this shapefile should be changed for the new project
LACity_Boundary = "Z:\GIS\DataLibrary\AdministrativeBoundaries\CityofLA\lLACity_Boundary.shp" #this will be used to clip the BOE intersections

##### Workspace #####
arcpy.Delete_management("in_memory")
#env.overwriteOutput = True  #so we can rerun this script and overwrite our output feature class
arcpy.CreateFileGDB_management("Z:/GIS/DataLibrary/Transportation/BOE_Centerline_Intersections150930", "boe_output.gdb") #create geodatabase
env.workspace = "Z:/GIS/DataLibrary/Transportation/BOE_Centerline_Intersections150930/boe_output.gdb" #set workspace

##### Setup Field Mapping & Add to Geodatabase #####
fms = arcpy.FieldMappings() #create FieldMappings Object
fms.addTable(shp) 

for fi in range(0,2): #we only want to convert fields AssetID & CL_NODE_ID to string values   
    fmap = fms.getFieldMap(fi) #get the field map for that field 
    ofield = fmap.outputField  
    ofield.type = "String" #change to string
    ofield.length = 20
    fmap.outputField = ofield  
    fms.replaceFieldMap(fi, fmap)
    print "finished the field map for %d" % (fi)  

arcpy.FeatureClassToFeatureClass_conversion(shp, env.workspace, "raw_intersections", field_mapping=fms) #add shp to gdb
# i want to edit this section because i the future we don't want to clip the intersections to the LA City boundary
#arcpy.Clip_analysis("raw_intersections", LACity_Boundary, "StreetIntersections") #clip shp to LACity_Boundary, add to gdb
print "finished clipping to LA City Boundary"
boe_int = "StreetIntersections" 

##### Clean Intersection Table #####
#1) remove all rows that have empty values for both TO/FROM, export to gdb 
#2) remove all rows that have second value as D/E, export to gdb
#3) organize remaining rows alphabetically to help with identifying duplicates

boe_int_lyr = "StreetIntersections_lyr"
expression_Empty = """ "FROM_ST" = '' AND "TO_ST" = '' """
expression_DeadEnd = """ "TO_ST" = 'D/E' """
expression_Fwy = """ "TOOLTIP" LIKE '%FRWY%' """
arcpy.MakeFeatureLayer_management(boe_int, boe_int_lyr) 
arcpy.FeatureClassToFeatureClass_conversion(boe_int_lyr, env.workspace, "boe_int_empty", expression_Empty) #create fc of all intersections with empty values for "TO" and "FROM"
arcpy.FeatureClassToFeatureClass_conversion(boe_int_lyr, env.workspace, "boe_int_DE", expression_DeadEnd) #create fc for all dead-ends
arcpy.FeatureClassToFeatureClass_conversion(boe_int_lyr, env.workspace, "boe_int_fwy", expression_Fwy) #create fc for all dead-ends

arcpy.SelectLayerByAttribute_management(boe_int_lyr, "NEW_SELECTION", expression_Empty) #select and then delete from original layer the empty & D/E intersections
arcpy.SelectLayerByAttribute_management(boe_int_lyr, "ADD_TO_SELECTION", expression_DeadEnd)
arcpy.SelectLayerByAttribute_management(boe_int_lyr, "ADD_TO_SELECTION", expression_Fwy)
if int(arcpy.GetCount_management(boe_int_lyr).getOutput(0)) > 0: 
    arcpy.DeleteRows_management(boe_int_lyr)
print "Removed D/Es, Freeway, and Empty Intersection Points"

From_St = [] #this process will alphabetize the from/to fields to help identify duplicates
To_St = [] 
rows = arcpy.SearchCursor(boe_int_lyr) #find the empty rows, delete in table
emptyList = []
for row in rows:
    curRow_from = str(row.FROM_ST)
    curRow_to = str(row.TO_ST)
    if curRow_from.upper() < curRow_to.upper():
        From_St.append(curRow_from.upper())
        To_St.append(curRow_to.upper())
    else:
        From_St.append(curRow_to.upper())
        To_St.append(curRow_from.upper())
del rows

rows = arcpy.UpdateCursor(boe_int_lyr) #go back and update to/from st attributes
for i, row in enumerate(rows):
    row.FROM_ST = str(From_St[i])
    row.TO_ST = str(To_St[i])
    rows.updateRow(row)
del rows
print "Finished updating To/From Column"

##### Find Identical & Join ##### 
xy_tol = "200 Feet"
identical_tbl = "boe_int_identical_tbl"
find_identical_result = arcpy.FindIdentical_management(boe_int_lyr, identical_tbl, ["FROM_ST","TO_ST","Shape"], xy_tol, output_record_option="ONLY_DUPLICATES") #need to change to all
arcpy.JoinField_management(identical_tbl, "IN_FID", boe_int_lyr, "OBJECTID") #join data from boe table to new identical table

##### Organize Identical Points from StreetIntersections FC ##### 
Asset_ID_List = [] #make a list of all the assets that were identified as identical; then delete those assets from the boe_int file
Feat_Seq_List = [] #list for the FEAT_SEQ (identifies similar points)
rows = arcpy.SearchCursor(identical_tbl) #grab the AssetID and FEAT_SEQ from Identical Table
for row in rows:
    Asset_ID_List.append(row.ASSETID)
    Feat_Seq_List.append(row.FEAT_SEQ)
del rows

##### Create FC of Duplicate Points##### 
boe_int_dup = "boe_int_duplicate" 
arcpy.SelectLayerByAttribute_management (boe_int_lyr, "CLEAR_SELECTION") #clear selection
AssetID_string = ",".join("'%s'" % id for id in Asset_ID_List) #create a list of strings, each string is the assetid
expression_Duplicate = ' "ASSETID" IN ' + '(' + AssetID_string + ')'
arcpy.FeatureClassToFeatureClass_conversion(boe_int_lyr, env.workspace, boe_int_dup, expression_Duplicate) #create fc for all duplicate points

##### Delete Identical Points from StreetIntersections FC ##### 
boe_int_dup_lyr = "boe_int_dup_lyr" #new feature lyr for duplicate points
arcpy.MakeFeatureLayer_management(boe_int_dup, boe_int_dup_lyr) #instead of creaing a fc, create a feature layer for duplicate points. export to fc later
arcpy.SelectLayerByAttribute_management (boe_int_lyr, "NEW_SELECTION", expression_Duplicate) #select all rows with asset id in the list, then delete
if int(arcpy.GetCount_management(boe_int_lyr).getOutput(0)) > 0: 
    arcpy.DeleteRows_management(boe_int_lyr)
print "Removed Duplicate Intersection Points from StreetIntersections"
arcpy.JoinField_management(boe_int_dup_lyr, "ASSETID", identical_tbl, "ASSETID",["FEAT_SEQ"]) #add FEAT_SEQ to boe_int_dup from identical_tbl
print "Joined FEAT_SEQ to boe_int_dup_lyr"

##### Concatenate IDs from Identical Data #####
Asset_IDs = [] #lists that we will populate then use w/ UpdateCursor later
Node_IDs = []
prevRow_Feat = "" #empty values just for row 1
prevRow_Asset = ""
prevRow_Cl = ""

rows = arcpy.SearchCursor(boe_int_dup_lyr, sort_fields="FEAT_SEQ A") #create the search cursor to loop through the table & append combined IDs to table
for row in rows:
    print row.ASSETID
    curRow_Feat = row.FEAT_SEQ
    curRow_Asset = row.ASSETID
    curRow_Cl = row.CL_NODE_ID
    if curRow_Feat == prevRow_Feat:
        #append it twice for each attribute, for each row in the pair
        Asset_IDs.append(str(int(curRow_Asset)) + "_" + str(int(prevRow_Asset)))
        Asset_IDs.append(str(int(curRow_Asset)) + "_" + str(int(prevRow_Asset)))
        Node_IDs.append(str(int(curRow_Cl)) + "_" + str(int(prevRow_Cl)))
        Node_IDs.append(str(int(curRow_Cl)) + "_" + str(int(prevRow_Cl)))
    #set the prev_row to equal the value of the current row
    prevRow_Feat, prevRow_Asset, prevRow_Cl = curRow_Feat, curRow_Asset, curRow_Cl
del rows

rows = arcpy.UpdateCursor(boe_int_dup_lyr, sort_fields="FEAT_SEQ A") # Copy the concatenated IDs back to identical_tbl
for i, row in enumerate(rows):
    row.ASSETID = str(Asset_IDs[i])
    row.CL_NODE_ID = str(Node_IDs[i])
    rows.updateRow(row) 
del rows

##### Integrate Identical Data #####
maxValue = arcpy.SearchCursor(boe_int_dup_lyr, "", "", "", "FEAT_SEQ D").next().getValue("FEAT_SEQ") #Get max FEAT_SEQ
for i in range(1, maxValue + 1):
    query = "\"FEAT_SEQ\" = " + str(i)
    print query
    arcpy.SelectLayerByAttribute_management(boe_int_dup_lyr, "NEW_SELECTION", query) 
    arcpy.Integrate_management(boe_int_dup_lyr, "200 feet") 
arcpy.SelectLayerByAttribute_management (boe_int_dup_lyr, "CLEAR_SELECTION") #clear selection so that the conversion includes all rows
print "finished integrating"

boe_int_dup_merge = "boe_int_dup_merge" 
arcpy.FeatureClassToFeatureClass_conversion(boe_int_dup_lyr, env.workspace, boe_int_dup_merge) #create fc for all duplicate points
arcpy.DeleteIdentical_management(boe_int_dup_merge, ["ASSETID"])
print "exported integrated fc"
