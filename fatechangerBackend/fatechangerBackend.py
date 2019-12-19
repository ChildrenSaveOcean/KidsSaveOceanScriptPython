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
from logging import exception

    


def mapNodeNamesToCreationMethods():
    # modify code to match Firebase database
    global nodesToMethods
    nodesToMethods = {"ACTIONS" : createActionNode,
                      "COUNTRIES" : createCountriesNode, 
                      "POLICIES" : createPoliciesNode,
                      "CAMPAIGNS" : createCampaignsNode,
                      "HIJACK_POLICIES" : createHijackPoliciesNode,
                      "HIJACK_POLICY_LOCATIONS" : createHijackPolicyLocationsNode
                    }

def downloadFateChangerFirebase():

    global nodeAll
    global colHeaders
    actionsDatabaseRef = db.reference("ACTIONS")
    policiesDatabaseRef = db.reference("POLICIES")
    countriesDatabaseRef = db.reference("COUNTRIES")
    campaignsDatabaseRef = db.reference("CAMPAIGNS")
    hijackPoliciesDatabaseRef = db.reference("HIJACK_POLICIES")
    hijackPolicyLocationsRef = db.reference("HIJACK_POLICY_LOCATIONS")
    nodeAll = {}
    nodeAll["ACTIONS"] = actionsDatabaseRef.order_by_key().get()
    nodeAll["POLICIES"] = policiesDatabaseRef.order_by_key().get()
    nodeAll["COUNTRIES"] = countriesDatabaseRef.order_by_key().get()
    nodeAll["CAMPAIGNS"] = campaignsDatabaseRef.order_by_key().get()
    nodeAll["HIJACK_POLICIES"] = hijackPoliciesDatabaseRef.order_by_key().get()
    nodeAll["HIJACK_POLICY_LOCATIONS"] = hijackPolicyLocationsRef.order_by_key().get()
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


def createFateChangerWorkbook(reportName):
    #===========================================================================
    # Creates ksoreports.xlsx file and ksousers.csv file from Firebase database
    #===========================================================================
    ksoReportsFileName = home+"/KSO/"+reportName+".xlsx"
    ksoReportsFilePath = Path(ksoReportsFileName)
    if ksoReportsFilePath.exists():
        # rename 
        ksoTime = datetime.datetime.today().strftime("%Y%m%d_%H%M%S%f")
        ksoReportsFilePath.rename(home + "/KSO/" + reportName + "_" + ksoTime + ".xlsx")
    # process every child of Firebase's root node
    writer = ExcelWriter(ksoReportsFileName)
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
    
def createUsersFile(usersName):
    #===========================================================================
    # Write USERS node to CSV file because of Excel's limit of 1M rows
    #===========================================================================
    ksoUsersFileName = home + '/KSO/' + usersName + '.csv'
    ksoUsersFilePath = Path(ksoUsersFileName)
    try:
        if ksoUsersFilePath.exists():
            # rename
            ksoTime = datetime.datetime.today().strftime("%Y%m%d_%H%M%S%f")
            ksoUsersFilePath.rename(home+"/KSO/" + usersName + "_" + ksoTime + ".csv")
        usersDatabaseRef = db.reference("USERS")
        nodeUsers = usersDatabaseRef.order_by_key().get()
        with open(ksoUsersFilePath, 'w') as ksoUsersFile:
            lineA =  "UID, dash_joined_a_policy_hijack_campaign, dash_learn_about_problem, dash_protest, dash_share"
            lineB = ", dash_wrote_a_letter_about_climate, dash_wrote_a_letter_about_plastic, location_id, signatures_pledged, user_letters_written, user_person_type"
            lineC = ", campaign_id, signatures_collected, hijack_policy_selected"
            line = lineA + lineB + lineC
            ksoUsersFile.write(line + '\n') # header line
            ksoHeaders = line.split(sep=', ') # create list of column headers
            for uid, userData in nodeUsers.items():
                line = ["Missing Data"]*len(ksoHeaders)
                if 'campaign' in userData:
                    # campaign is optional for students
                    studentCampaign = userData.pop('campaign')
                    if 'campaign_id' in studentCampaign:
                        ksoSet(ksoHeaders, line, 'campaign_id', studentCampaign['campaign_id'])
                    if 'signatures_collected' in studentCampaign:
                        ksoSet(ksoHeaders, line, 'signatures_collected', studentCampaign['signatures_collected'])
                userValues = list(userData.values())
                userKeys = list(userData.keys())
                for i in range(len(userKeys)):
                    ksoSet(ksoHeaders, line, userKeys[i], userValues[i])
                # post UID
                line[0] = uid
                outLine = ','.join(map(str,line))
                ksoUsersFile.write(outLine+'\n')           
            ksoUsersFile.close()
    except Exception as err:
        print("Failure with error in createUsersFile(): " )
        print(err)
    
def createNode(nodeName):
    ref = db.reference(nodeName)
    nodeKey = nodeName + '_keys'
    nodeDataFrame = pd.read_excel(ksoFileName, sheet_name=nodeName)
    map = mapFirebaseFieldsToExcelColumns(nodeName, nodeDataFrame)
    firebaseRow = {}
    # post Excel data to Firebase
    isDeleteAction = True
    # process all data rows for Excel worksheet
    for i in range(len(nodeDataFrame.get_values())):
        # process rows of data
        for colName, colIndex in map.items():
            firebaseRow[colName] = nodeDataFrame.get_values()[i][colIndex]
            if pd.notna(nodeDataFrame.get_values()[i][colIndex]) and colName != nodeKey:
                isDeleteAction = False
        try:
            action = firebaseRow[nodeKey]
            del firebaseRow[nodeKey]
            if isDeleteAction:
                ref.child(action).delete()
            else:
                ref.update({action:firebaseRow})   
            isDeleteAction = True
        except Exception as err:
            print("error encountered in createNode(). Get technical help", err)
        
   
def createActionNode():
    createNode('ACTIONS')
        
        
def createPoliciesNode():
    createNode('POLICIES')
    
            
                
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
        #

        # prevent entry of invalid country code
        try:
                    # bug in pandas. "NA" key interpreted as nan
            if pd.isna(countriesDataFrame.get_values()[i][0]):
                 checkCountryCode ="NA"
            else:
                checkCountryCode = countriesDataFrame.get_values()[i][0]
            if len(countries.get(checkCountryCode)) == 0:
                # print warning
                printInstructionsForISO(countriesDataFrame.get_values()[i][0])
                continue
        except Exception as err:
                printInstructionsForISO(countriesDataFrame.get_values()[i][0])
                continue
        if len(checkCountryCode) != 2:
            printInstructionsForISO(checkCountryCode)
            continue
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
    
def printInstructionsForISO(countryCode):
    print('*' * 50)
    print(countryCode, " not found is ISO country codes. Row skipped. Please edit your worksheet data with a valid ISO country code.")
    print("If you know the country code is correct then the next step is to update the script's ISO data.")
    print("In a terminal window run this command, pip install iso3166")
    print('*' * 50)
        
def createCampaignsNode():
    # check foreign keys
    nodeName = "CAMPAIGNS"
    ref = db.reference(nodeName)
    nodeKey = nodeName + '_keys'
    nodeDataFrame = pd.read_excel(ksoFileName, sheet_name=nodeName)
    map = mapFirebaseFieldsToExcelColumns(nodeName, nodeDataFrame)
    firebaseRow = {}
    # post Excel data to Firebase
    isDeleteAction = True
    # find foreign key columns
    headers = list(nodeDataFrame.columns)
    fkColHijackLocationIndex = headers.index("location_id")
    fkColHijackPolicyIndex = headers.index("hijack_policy")
    campaignKeyIndex = headers.index("CAMPAIGNS_keys")
    nodeHijackPolicyLocations = nodeAll["HIJACK_POLICY_LOCATIONS"]
    nodeHijackPolicies = nodeAll["HIJACK_POLICIES"]
    # process all data rows for Excel worksheet
    for i in range(len(nodeDataFrame.get_values())):
        # process rows of data
        # add foreign key check here
        locationKey = nodeDataFrame.get_values()[i][fkColHijackLocationIndex]
        policyKey = nodeDataFrame.get_values()[i][fkColHijackPolicyIndex]
        if locationKey not in nodeHijackPolicyLocations.keys():
            print("Campaign (", nodeDataFrame.get_values()[i][campaignKeyIndex], ") location foreign key not in database. Skipped campaign")
            continue
        if policyKey not in nodeHijackPolicies.keys():
            print("Campaign (", nodeDataFrame.get_values()[i][campaignKeyIndex], ") hijack policy foreign key not in database. Skipped campaign")
            continue
        
        for colName, colIndex in map.items():
            firebaseRow[colName] = nodeDataFrame.get_values()[i][colIndex]
            if pd.notna(nodeDataFrame.get_values()[i][colIndex]) and colName != nodeKey:
                isDeleteAction = False
        try:
            action = firebaseRow[nodeKey]
            del firebaseRow[nodeKey]
            if isDeleteAction:
                ref.child(action).delete()
            else:
                ref.update({action:firebaseRow})   
            isDeleteAction = True
        except Exception as err:
            print("error encountered in createCampaignsNode(). Get technical help", err)

    
def createHijackPoliciesNode():
    createNode("HIJACK_POLICIES")
    
def createHijackPolicyLocationsNode():
    createNode("HIJACK_POLICY_LOCATIONS")

def ksoSet(headers, rowValues, matchByLabel, replacementValue):
    # verify a valid matchByLabel
    if matchByLabel in headers:
        ksoPos = headers.index(matchByLabel)
        rowValues[ksoPos] = replacementValue
    else: 
        print("rowValues: ", rowValues)
        print("Mismatch between script and Firebase for ", matchByLabel, ". Seek technical help")



def editFirebaseFields(firebaseRow):    
    
    checkAddress = firebaseRow["country_address"]
    if "\\n" in checkAddress:
        checkAddress = checkAddress.replace("\\n",'\n')
    # Post Firebase data
    firebaseRow["country_address"] = checkAddress
    # workaround bug in pandas for country code value of NA
    if pd.isna(firebaseRow["COUNTRIES_keys"]):
        firebaseRow["COUNTRIES_keys"] = "NA"
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

    
def createReports(reportName, usersName):
    print("Starting Fatechanger reports")
    createFateChangerWorkbook(reportName)
    createUsersFile(usersName)
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

def editChanges():
    print("Edit changes")
    
    

# Setup 
#
# Globals
#

#===============================================================================
# Main 
#===============================================================================
print("fatechangerBackend.py Version 1, run on ",datetime.datetime)
home = str(Path.home())
listOfNodes = []
nodeAll = {}
nodeSkipList = ["USERS", "PERSON_TYPE"]
colHeaders = {}
nodesToMethods = {}
# identify the Firebase database to use
ksoDatabase = "https://kids-save-ocean.firebaseio.com/"
# Fetch the service account key JSON file contents
adminSDKJSON = home + "/KSO/kids-save-ocean-firebase-adminsdk-g1fqp-abd71c2f01.json"
# identify the input file
ksoFileName = home + "/KSO/kso.xlsx"
# identify report file name
ksoReportName = "ksoReports"
# identify users files name
ksoUsersName = "ksoUsers"
# override parameters if test selected
if len(sys.argv) == 2:
    if "test" == sys.argv[1]:
        ksoDatabase="https://kids-save-ocean-test.firebaseio.com/"
        adminSDKJSON = home + "/KSO/kids-save-ocean-test-firebase-adminsdk-t8sfp-97dae1ed9e.json"
        ksoFileName = home + "/KSO/ksoTest.xlsx"
        ksoReportName ="ksoTestReports"
        ksoUsersName = "ksoTestUsers"
print("Using Firebase database: ", ksoDatabase)

adminPath = Path(adminSDKJSON)
if not adminPath.exists() :
    print("no Firebase key file found. Fix and rerun")
    exit()
else:
    print("Firebase key file found!")
cred = credentials.Certificate(adminSDKJSON) 
# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    # test'databaseURL' : 'https://kidssaveoceandatabase.firebaseio.com/'
    'databaseURL': ksoDatabase
})
# initialize Firebase access
mapNodeNamesToCreationMethods()
downloadFateChangerFirebase()

ksoFilePath = Path(ksoFileName)
if not ksoFilePath.exists() :
    print("no" + ksoFileName + " file found; creating reports only")
    createReports(ksoReportName, ksoUsersName)
else:
    print("opened opened input file, ", ksoFileName)
    wsProcessList = verifyWorkbook(ksoFileName)
    if len(wsProcessList) == 0:
        print("No valid worksheets located. Can't process update Firebase database")
    # update Firebase nodes
    editChanges()
    for ws in wsProcessList:
        runMethod = nodesToMethods[ws]
        runMethod()
    downloadFateChangerFirebase() # refresh Firebase data after updates processed
    createReports(ksoReportName, ksoUsersName) # produce reports

print("Finished FateChanger backend processing")
