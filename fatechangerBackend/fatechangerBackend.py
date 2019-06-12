import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import xlrd
from pathlib  import Path
from tokenize import String
import datetime
import csv
from iso3166 import countries
import sys
from matplotlib.backend_bases import _default_filetypes
import os
from docutils.utils.math.math2html import Newline
from pandas.core.config_init import colheader_justify_doc
from dill.dill import check
from math import isnan
from numpy.f2py.common_rules import findcommonblocks

    


def mapNodeNamesToCreationMethods():
    # modify code to match Firebase database
    global nodesToMethods
    nodesToMethods = {"ACTIONS" : createActionNode,
                      "COUNTRIES" : createCountriesNode, 
                      "POLICIES" : createPoliciesNode}

def downloadFateChangerFirebase():

    global nodeAll
    global colHeaders
    actionsDatabaseRef = db.reference("ACTIONS")
    policiesDatabaseRef = db.reference("POLICIES")
    countriesDatabaseRef = db.reference("COUNTRIES")
    nodeAll = {}
    nodeAll["ACTIONS"] = actionsDatabaseRef.order_by_key().get()
    nodeAll["POLICIES"] = policiesDatabaseRef.order_by_key().get()
    nodeAll["COUNTRIES"] = countriesDatabaseRef.order_by_key().get()
    colHeaders = {}
    for nodeName, node in nodeAll.items():
        # node is a top level branch of Firebase root database
        if nodeName in nodeSkipList:
            continue
        mapKeysToCols = {} 
        # ensure class a dictionary 
        if type(node) is list:
            newNode = {}
            for i in range(len(node)):
                newNode[i] = node[i]
            node = newNode
        # maps Firebase structure to Excel structure
        mapKeysToCols[nodeName+"_keys"] = len(mapKeysToCols)
        for nodeKey, nodeDict in node.items(): 
            # get key of node and its dictionary
            # nodeDict is a dictionary of fields
            # post node key first

            for keyToCol in nodeDict: # iterate through labels of fields
                if keyToCol not in mapKeysToCols:
                    mapKeysToCols[keyToCol] = len(mapKeysToCols) # order field positions
        # save each top level node's mapping from Firebase to Excel
        colHeaders[nodeName] = mapKeysToCols


def createFateChangerWorkbook():
    #===========================================================================
    # Creates ksoreports.xlsx file and ksousers.csv file from Firebase database
    #===========================================================================
    ksoReportsFileName = home + "/KSO/ksoReports.xlsx"
    ksoReportsFilePath = Path(ksoReportsFileName)
    if ksoReportsFilePath.exists():
        # rename 
        ksoTime = datetime.datetime.today().strftime("%Y%m%d_%H%M%S%f")
        ksoReportsFilePath.rename(home+"/KSO/ksoReports_" + ksoTime + ".xlsx")
    # process every child of Firebase's root node
    writer = ExcelWriter(home+'/KSO/ksoReports.xlsx')
    for nodeName, node in nodeAll.items():
        if nodeName in nodeSkipList:
            continue
        excelRows = []
        mapKeysToCols = colHeaders[nodeName]
        # ensure class a dictionary 
        if type(node) is list:
            newNode = {}
            for i in range(len(node)):
                newNode[i] = node[i]
            node = newNode
        for nodeKey, nodeDict in node.items():
            excelRow = [None]*len(mapKeysToCols) # prep Excel row
            excelRow[0] = nodeKey
            # extract each field and map to the correct Excel column
            for fieldKey, fieldValue in nodeDict.items():
                excelColIndex = mapKeysToCols[fieldKey]
                excelRow[excelColIndex] = fieldValue
            excelRows.append(excelRow)
        excelColHeadings = []
        #excelColHeadings.append(nodeName)
        excelColHeadings.extend(list(colHeaders[nodeName].keys()))
        excelRowDictionary = {}
        for excelRowIndex in range(len(excelRows)):
            excelRowDictionary[excelRowIndex] = excelRows[excelRowIndex] # prepare Excel row for adding to dataframe
        nodeDataFrame = pd.DataFrame.from_dict((excelRowDictionary), orient='index', columns=excelColHeadings)
        nodeDataFrame.to_excel(writer, sheet_name = nodeName, index=False)
    writer.close()
    
def createUsersFile():
    #===========================================================================
    # Write USERS node to CSV file because of Excel's limit of 1M rows
    #===========================================================================
    ksoUsersFileName = home + '/KSO/ksoUsers.csv'
    ksoUsersFilePath = Path(ksoUsersFileName)
    try:
        if ksoUsersFilePath.exists():
            # rename
            ksoTime = datetime.datetime.today().strftime("%Y%m%d_%H%M%S%f")
            ksoUsersFilePath.rename(home+"/KSO/ksoUsers_" + ksoTime + ".csv")
        usersDatabaseRef = db.reference("USERS")
        nodeUsers = usersDatabaseRef.order_by_key().get()
        with open(ksoUsersFilePath, 'w') as ksoUsersFile:
            line = "UID, dash_become_active_in_local_politics, dash_learn_about_problem, dash_protest, dash_share, dash_start_a_letter_writing_campaign, dash_write_a_letter,user_letters_written, user_person_type"
            ksoUsersFile.write(line + '\n')
            for uid, userData in nodeUsers.items():
                line =  ""
                userValues = list(userData.values())
                if len(userValues) == 8:
                    csvLine = [] # prep output line
                    csvLine.append(uid) # post UID
                    csvLine.extend(userValues)
                    line = ','.join(str(x) for x in csvLine)
                else:
                    line = "User UID ("+uid+") doesn't expected number of data items. Seek technical help"
                ksoUsersFile.write(line+'\n')           
            ksoUsersFile.close()
    except Exception as err:
        print("Failure with error in createUsersFile(): " )
        print(err)
    

    
def createActionNode():
    ref = db.reference('ACTIONS')
    actionsDataFrame = pd.read_excel(ksoFileName, sheet_name="ACTIONS")
    map = mapFirebaseFieldsToExcelColumns("ACTIONS", actionsDataFrame)
    firebaseRow = {}
    # post Excel data to Firebase
    isDeleteAction = True
    # process all data rows for Excel worksheet
    for i in range(len(actionsDataFrame.get_values())):
        # process rows of data
        for colName, colIndex in map.items():
            firebaseRow[colName] = actionsDataFrame.get_values()[i][colIndex]
            if pd.notna(actionsDataFrame.get_values()[i][colIndex]) and colName != "ACTIONS_keys":
                isDeleteAction = False
        try:
            action = firebaseRow["ACTIONS_keys"]
            del firebaseRow["ACTIONS_keys"]
            if isDeleteAction:
                ref.child(action).delete()
            else:
                ref.update({action:firebaseRow})   
            isDeleteAction = True
        except Exception as err:
            print("error encountered in CreateActionNode(). Get technical help", err)
        
        
def createPoliciesNode():
    ref = db.reference('POLICIES')
    policiesDataFrame = pd.read_excel(ksoFileName, sheet_name="POLICIES")
    map = mapFirebaseFieldsToExcelColumns("POLICIES", policiesDataFrame)
    firebaseRow = {}
    # post Excel data to Firebase
    isDeleteAction = True
    # process all data rows for Excel worksheet
    for i in range(len(policiesDataFrame.get_values())):
        # process rows of data
        for colName, colIndex in map.items():
            firebaseRow[colName] = policiesDataFrame.get_values()[i][colIndex]
            if pd.notna(policiesDataFrame.get_values()[i][colIndex]) and colName != "POLICIES_keys":
                isDeleteAction = False
        try:
            policy = firebaseRow["POLICIES_keys"]
            del firebaseRow["POLICIES_keys"]
            if isDeleteAction:
                ref.child(policy).delete()
            else:
                ref.update({policy:firebaseRow})   
            isDeleteAction = True
        except Exception as err:
            print("error encountered in createPoliciesNode(). Get technical help", err)
        
            
                
def createCountriesNode():

    # point to COUNTRIES node in Firebase
    ref = db.reference('COUNTRIES')
    countriesDataFrame = pd.read_excel(ksoFileName, sheet_name="COUNTRIES")
    map = mapFirebaseFieldsToExcelColumns("COUNTRIES", countriesDataFrame)
    firebaseRow = {}
    # post Excel data to Firebase
    isDeleteAction = True
    # process all data rows for Excel worksheet
    for i in range(len(countriesDataFrame.get_values())):
        # process rows of data
        for colName, colIndex in map.items():
            # seed check
            if colIndex == len(countriesDataFrame.get_values()[i]):
                break
            firebaseRow[colName] = countriesDataFrame.get_values()[i][colIndex]
            if pd.notna(countriesDataFrame.get_values()[i][colIndex]) and colName != "COUNTRIES_keys":
                isDeleteAction = False
        try:
            if isDeleteAction:
                ref.child(countriesDataFrame.get_values()[0][i]).delete()
            else:
                editedFirebaseRow = editFirebaseFields(firebaseRow)
                if editedFirebaseRow == None:
                    continue
                country = editedFirebaseRow["COUNTRIES_keys"]
                del editedFirebaseRow["COUNTRIES_keys"]
                ref.update({country:editedFirebaseRow})   
            isDeleteAction = True
        except Exception as err:
            print("error encountered in createCountries(). Get technical help", err)
    
    
    
def editFirebaseFields(firebaseRow):    
    
    checkAddress = firebaseRow["country_address"]
    if "\\n" in checkAddress:
        checkAddress = checkAddress.replace("\\n",'\n')
    # Post Firebase data
    firebaseRow["country_address"] = checkAddress
    return firebaseRow

def mapFirebaseFieldsToExcelColumns(nodeName, dataFrame): 
    # given a Firebase node, map fields to Excel dataFrame with matching names
    
    validFirebaseFields = list(colHeaders[nodeName].keys()) # list of Firebase field names
    mapFirebaseToExcel = {}
    # get names of columns from the Excel dataFrame
    fieldPositionInExcel = {}
    for colIndex in range(len(dataFrame.columns)):
        if dataFrame.columns[colIndex] in validFirebaseFields:
            mapFirebaseToExcel[dataFrame.columns[colIndex]] = colIndex
    # first column 
    if len(validFirebaseFields) != len(mapFirebaseToExcel):
        print("Missing Firebase fields for ", nodeName, " in Excel worksheet. Check work and rerun script")
    return mapFirebaseToExcel

    
def createReports():
    print("Starting Fatechanger reports")
    createFateChangerWorkbook()
    createUsersFile()
    print("Finished Fatechanger reports")
    
def verifyWorkbook(ksoFileName):
    # verify worksheets name
    wbKSO = pd.ExcelFile(ksoFileName)
    wsKSO = wbKSO.sheet_names
    fbNodes = list( colHeaders.keys() )
    processWorksheets = []
    for ws in wsKSO:
        if ws not in fbNodes:
            print("Worksheet ", ws, "not a valid Firebase node name. Skipping")
        else:
            processWorksheets.append(ws)
    return processWorksheets


# Setup 
#
# Globals
#

#===============================================================================
# Main 
#===============================================================================
home = str(Path.home())
listOfNodes = []
nodeAll = {}
nodeSkipList = ["USERS", "PERSON_TYPE"]
colHeaders = {}
nodesToMethods = {}

# Fetch the service account key JSON file contents
adminSDKJSON = home + "/KSO/kids-save-ocean-firebase-adminsdk-g1fqp-abd71c2f01.json"
adminPath = Path(adminSDKJSON)
if not adminPath.exists() :
    print("no Firebase key file found. Fix and rerun")
    exit()
else:
    print("Firebase key file found!")
cred = credentials.Certificate(adminSDKJSON) 
# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://kids-save-ocean.firebaseio.com/'
})
# initialize Firebase access
mapNodeNamesToCreationMethods()
downloadFateChangerFirebase()
ksoFileName = home + "/KSO/kso.xlsx"
ksoFilePath = Path(ksoFileName)
if not ksoFilePath.exists() :
    print("no kso.xlsx file found, creating reports only")
    createReports()
else:
    print("opened opened input file, ", ksoFileName)
    wsProcessList = verifyWorkbook(ksoFileName)
    if len(wsProcessList) == 0:
        print("No valid worksheets located. Can't process update Firebase database")
    # update Firebase nodes
    for ws in wsProcessList:
        runMethod = nodesToMethods[ws]
        runMethod()
    downloadFateChangerFirebase() # refresh Firebase data after updates processed
    createReports() # produce reports

print("Finished FateChanger backend processing")
