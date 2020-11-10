import BLS_Request
import os
import pandas as pd
import csv
#path: Dynamic path which is the current directory where the pc.py program is located.
path = str(os.path.dirname(os.path.realpath(__file__)))
#QuartersArr: Contains the quarters used in the quartising function. 
quartersArr = ["M01M02M03","M04M05M06","M07M08M09","M10M11M12"]
pcProxy = {}
#Quarter class: Used in the calculation of the quarterly values.
class quarters:
    def __init__(self, q1, q2, q3, q4):
        self.q1 = q1
        self.q2 = q2
        self.q3 = q3
        self.q4 = q4
        
#Gets the latest version of the pc.data.0.Current using the BLS_Request library located in BLS_Request.py
def checkForLatestVersion():
    # Compares the latest version online with the latest version downloaded, if the online version is newer, the online one is downloaded.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current",pcProxy)

#Reads a parquet file and turns it into pyarrow table, then .to_pandas() converts the table to a dataframe.
def readCSV(fileName):
    # - fileName: (String) The name of the parquet file to be read and converted from parquet to pandas.
    return pd.read_csv(fileName)

# Formats the date from a separate year / month (period) to a string that is in the yyyy-mm-01 format.
def formatTimePeriod(year,monthPeriod):
    # monthPeriod[1:]: (string) Removes the M from the MXX period string to leave the numbers.
    return str(year) + "-" + str(monthPeriod[1:]) + "-01"

# Rounds the resulting difference from currentNum - previousNum
def specialRounding(currentNum, previousNum):
    # - currentNum: (Float) The latest number of the 2
    # - previousNum: (Float) The other number
    return round((currentNum-previousNum),1)   

# Performs the year over year calculations to determine the changes between the same months of consecutive years (Ex. March 2019 to March 2020)
def yearOverYearCalculation(dataFrame,dropM13):
    # - dataFrame: (Dataframe) The dataframe containing all the information
    # - dropM13: (Integer) Indicates whether or not the rows containing the M13 have been dropped. 
    # Initialises the yearOverYear array
    yearOverYear = []
    # Sorts the dataframe by period then year
    dataFrame = dataFrame.sort_values(by=["period","year"])
    # Populates the blank yearOverYear array with empty strings.
    for i in range(0,len(dataFrame)):
        yearOverYear.append("")
    # Attaches the new yearOverYear column to the current dataFrame.
    dataFrame.insert(3,"yearOverYear",yearOverYear,True)
    # Groups the content of the dataframe by the series_id
    grouped = dataFrame.groupby("series_id")
    # Initialises the new dataframe
    newDF = []
    # Iterates through the grouped dataframe.
    for group in grouped:
        tempGroup = group[1]
        # For each grouped dataframe...
        # Converts the dataframe to a 2d array.
        tempGroup = tempGroup.values.tolist()
        tempGroup[0][5] = "X"
        # Iterates through the individual group.
        for i in range(1,len(tempGroup)):
            # Checks if the year is greater than the year in the row above.
            if int(tempGroup[i][1]) > int(tempGroup[i-1][1]):
                # Rounds the difference between the year and the previous year with the same month.
                tempGroup[i][5] = specialRounding(float(tempGroup[i][4]),float(tempGroup[i-1][4]))
            else:
                tempGroup[i][5] = "X"
        # Iterates through the modified tempGroup
        for i in tempGroup:
            # Adds the modified tempGroup row to the newDF
            newDF.append(i)
    # Creates a new dataframe from the newDF 2d array.
    newFrame = pd.DataFrame(newDF,columns=["series_id","year","period","footnote_codes","value","yearOverYear"])
    # Sorts the new dataframe.
    newFrame = newFrame.sort_values(by=["series_id","year"])
    return newFrame

# QuarteriseDataFrame: Converts the dataframe from monthly periods to quarters. 
def quarteriseDataFrame(dataFrame):
    # Converts the dataframe to a 2D array.
    dfList = dataFrame.values.tolist()
    # Initialises the newDF array.
    newDF = []
    # Initialises the quarterDict
    quarterDict = {}
    # Iterates through the dfList
    for row in range(0,len(dfList)):
        # Checks if the series_id is in the quarterdict
        if dfList[row][0] not in quarterDict:
            # Creates a nested dictionary with the key as the series_id
            quarterDict[dfList[row][0]] = {}
        # Checks if the series_id is in the quarterDict and if it is, adds an entry in the nested dictionary with the keys being the series_id and the year.
        if dfList[row][1] not in quarterDict[dfList[row][0]]:
            quarterDict[dfList[row][0]][dfList[row][1]] = quarters([],[],[],[])
        # Iterates through the quartersArr
        for m in range(0,len(quartersArr)):
            # Checks if the current row's period is in current quarter in the quartersArr.
            if dfList[row][2] in quartersArr[m]:
                if m == 0:
                    quarterDict[dfList[row][0]][dfList[row][1]].q1.append(float(dfList[row][3]))
                elif m == 1:
                    quarterDict[dfList[row][0]][dfList[row][1]].q2.append(float(dfList[row][3]))
                elif m == 2:
                    quarterDict[dfList[row][0]][dfList[row][1]].q3.append(float(dfList[row][3]))
                elif m == 3:
                    quarterDict[dfList[row][0]][dfList[row][1]].q4.append(float(dfList[row][3]))
    # Iterates through the outer dictionary.
    for x in quarterDict:
        # Iterates through the nested dictionar 
        for k in quarterDict[x]:
            # Adds each of the quarter arrays to the newDF.
            newDF.append([x,k,"Q1",arrayAvg(quarterDict[x][k].q1)])
            newDF.append([x,k,"Q2",arrayAvg(quarterDict[x][k].q2)])
            newDF.append([x,k,"Q3",arrayAvg(quarterDict[x][k].q3)])
            newDF.append([x,k,"Q4",arrayAvg(quarterDict[x][k].q4)])
    # Creates the new dataframe from the 2d array.
    newDataFrame = pd.DataFrame(newDF, columns=["series_id","year","quarter","value"])
    # Removes the rows from the dataframe where the value == X
    newDataFrame = newDataFrame[newDataFrame["value"]!="X"]
    newDataFrame["year"] = newDataFrame["year"].astype(str)
    newDataFrame["quarter"] = newDataFrame["quarter"].astype(str)
    newDataFrame["reference_period"] = newDataFrame["year"] + newDataFrame["quarter"]
    newDataFrame = newDataFrame.drop(columns=["year","quarter"])
    return newDataFrame

# Gets the average of the values in the array.
def arrayAvg(arr):
    # An array of numbers
    if len(arr) == 0:
        return "X"
    return round(sum(arr)/len(arr),1)

# periodOverPeriodCalculation: Calculates the difference between consecutive time periods
def periodOverPeriodCalculation(dataFrame):
    # converts the dataframe to a 2d Array
    dfList = dataFrame.values.tolist()
    # Initialises a blank array to store the percentage differences.
    percentageColumn = []
    # Initialises the labelDict
    labelDict = []
    # Iterates through dfList
    for row in range(0,len(dfList)):
        # Checks if the series_id is in the labeldict
        if dfList[row][0] not in labelDict:
            labelDict.append(dfList[row][0])
            # Adds a blank placeholder (X) to the percentage column
            percentageColumn.append("X")
        # Checks if the current value in value column or the one above it is a blank placeholder
        elif dfList[row][dataFrame.columns.get_loc("value")] == "X" or dfList[row-1][dataFrame.columns.get_loc("value")] == "X":
            percentageColumn.append("X")
        else:
        # Adds the difference between the two values to the percentage column.
            percentageColumn.append(specialRounding(float(dfList[row][dataFrame.columns.get_loc("value")]),float(dfList[row-1][dataFrame.columns.get_loc("value")])))
    # Adds the percentage column to the dataframe
    dataFrame.insert((dataFrame.columns.get_loc("value")+1),"percent_change",percentageColumn,True)
    return dataFrame

# Makes the dataframe from monthly (period based) into year based ones.
def yearifyDataFrame(dataFrame):
    # Initialises the blank newDF
    newDF = []
    # converts the dataframe to a 2d Array
    dfList = dataFrame.values.tolist()
    # Initialises the yearDict
    yearDict = {}
    # Iterates through dfList
    for row in range(0,len(dfList)):
        # Checks if the series_id is in the labeldict
        if dfList[row][0] not in yearDict:
            # Initialises an empty dictionary with the key being the series_id
            yearDict[dfList[row][0]] = {}
        # Checks if the year is in the dictionary that has the key series_id
        if dfList[row][1] not in yearDict[dfList[row][0]]:
            # Creates a blank array in the nested dictionary.
            yearDict[dfList[row][0]][dfList[row][1]] = []
        # Appends the value to the array in the nested dictionary.
        yearDict[dfList[row][0]][dfList[row][1]].append(float(dfList[row][3]))
    # Iterates through the outer dictionary
    for x in yearDict:
        # Iterates through the inner dictionary.
        for k in yearDict[x]:
            # Adds the new row which has the series_id, year, and averaged year value.
            newDF.append([x,k,arrayAvg(yearDict[x][k])])
    return pd.DataFrame(newDF, columns=["series_id","year","value"])

# Takes the original dataframe information and converts in into a custom formatted dataframe.
def createCustomFormattedDataFrame(dataFrame,inputArray):
    # Initialises the values to avoid problems with use before initialisation. 
    avgOverYear = inputArray[0]
    avgOverQrt = inputArray[1]
    m13Drop = inputArray[3]
    timeFormat = inputArray[4]
    seasonColumn = inputArray[5]
    yearOverYearBool = inputArray[6]
    percentageChg = inputArray[7]
    labelAdd = inputArray[8]
    codeSplit = inputArray[9]
    dataFrameMelting = inputArray[10]
    if avgOverQrt == False:
        if avgOverYear == True:
            # dataframe gets replaced with a dataframe in the "yearified" format.
            dataFrame = yearifyDataFrame(dataFrame)
        else:
            if yearOverYearBool == True:
                # Returns the dataframe with the year over year calculations added.
                dataFrame = yearOverYearCalculation(dataFrame,m13Drop)
    if avgOverQrt == True:
        # Converts the dataframe from periods into quarters
        dataFrame = quarteriseDataFrame(dataFrame)
    # Initialises the arrays 
    seasonal = []
    groupCode = []
    itemCode = []
    if percentageChg == True:
        # Returns the dataframe with period over period calculations performed.
        dataFrame = periodOverPeriodCalculation(dataFrame)
    if labelAdd == True:
        # Gets the group labels using the BLS_Request library.
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcInd","groupLabels",pcProxy)
        # Gets the item labels using the BLS_Request library.
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcLRef","labels",pcProxy)
        # Creates the paths for the for the item labels and the group labels
        newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcLRef",BLS_Request.getAllFilesInDirectory("pcLRef")))
        newGroupPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcInd",BLS_Request.getAllFilesInDirectory("pcInd")))
        # Modifies the row headers for the two data frames.
        newGroupFrame = changeRowHeaders(readCSV(newGroupPath))
        newDataFrame = changeRowHeaders(readCSV(newPath))
        # Merges the two dataframes using a left join.
        mergeLeft = pd.merge(left=newGroupFrame,right=newDataFrame,how='left',left_on='industry_code',right_on='industry_code')
        # Iterates through the rows by the dataframe index.
        for row in dataFrame.index:
            # gets the series_id from each row
            columnRow = dataFrame["series_id"][row]
            # adds the seasonal letter to the seasonal array
            seasonal.append(columnRow[2:3])
            groupCode.append(columnRow[3:9])
            itemCode.append(columnRow[9:])
        # Checks if the seasonColumn is wanted
        if seasonColumn == True:
            # Attachs the season column.
            dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+1),"seasonal", seasonal, True)
        # Attachs the group code to the dataframe
        dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+(int(seasonColumn==True))+1),"industry_code",groupCode,True)
        # Attachs the item code to the dataframe
        dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+(int(seasonColumn==True))+2),"product_code",itemCode,True)
        # Performs a left join on the dataframe and the mergeLeft dataframe to add the labels. 
        dataFrame = pd.merge(left=dataFrame,right=mergeLeft,how='left',left_on=['industry_code','product_code'],right_on=['industry_code','product_code'])
        # Gets the list of headers
        listOfHeaders = list(dataFrame.columns)
        # Sets the item at the group_name index to item_code
        listOfHeaders[listOfHeaders.index("industry_name")] = "product_code"
        # Sets the item at the item_code index to group_name
        listOfHeaders[listOfHeaders.index("product_code")] = "industry_name"
        # Reindexes the dataframe based on the modified list of headers.
        dataFrame = dataFrame.reindex(columns=listOfHeaders)
    elif codeSplit == True:
        # Iterates through the rows by the dataframe index.
        for row in dataFrame.index:
            # gets the series_id from each row
            columnRow = dataFrame["series_id"][row]
            # adds the seasonal letter to the seasonal array
            seasonal.append(columnRow[2:3])
            groupCode.append(columnRow[3:9])
            itemCode.append(columnRow[9:])
        # Checks if the seasonColumn is wanted
        if seasonColumn == True:
            # Attachs the season column to the dataframe
            dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+1),"seasonal", seasonal, True)
        # Attachs the group code to the dataframe
        dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+(int(seasonColumn==True))+1),"industry_code",groupCode,True)
        # Attachs the item code to the dataframe
        dataFrame.insert((dataFrame.columns.get_loc("value")+(int(percentageChg==True))+(int(seasonColumn==True))+2),"product_code",itemCode,True)
    # Checks if the quartised or yearified functions haven't been used and the dataframe is in standard period format.
    if avgOverQrt == False and avgOverYear == False:
        # Check if m13 is to be dropped
        if m13Drop == True:
            # Drops all entries that have a period equal to M13 from the dataframe
            dataFrame = dataFrame[dataFrame.period != "M13"]
        # Checks if the user wants the time formatted in yyyy-mm-01 format.
        if timeFormat == True:
            # Initialises an array for the formatted time.
            formattedTime = []
            # Iterates through the dataframe by index.
            for row in dataFrame.index:
                # Formats the time period based on the parameters and appends it to the formatted time array.
                formattedTime.append(formatTimePeriod(dataFrame["year"][row],dataFrame["period"][row]))
            # Attachs the formatted_time array to the dataframe.
            dataFrame.insert(1,"formatted_time",formattedTime,True)
            # Drops the year and period columns as formatted time replaces them.
            dataFrame = dataFrame.drop(['year','period'],axis=1)
    if dataFrameMelting == True:
        # Returns the melted dataframe.
        return wideFormat(dataFrame,avgOverQrt,avgOverYear,timeFormat,percentageChg,yearOverYearBool)
    else:
        return dataFrame

def modifyHeaders(dataFrame):
    newColumns = []
    for i in dataFrame.columns:
        if isinstance(i,tuple):
            labelStr = ""
            for j in range(0,len(i)):
                if j == 0:
                    labelStr += str(i[j])
                else:
                    labelStr += "_" + str(i[j])
            newColumns.append(labelStr)
        else:
            newColumns.append(i)
    return newColumns

# Converts the standard dataframe into the wide format.
def wideFormat(dataframe,avgQrt,avgYear,timeForm,percentageChg,yearToDrop):
    # Initialises the columnTitle
    columnTitle = []
    # Iterates through the dataframe columns. 
    for col in dataframe.columns:
        # Appends the col to the columnTitle array
        columnTitle.append(col)
    # Checks if the user has had the data formatted by quarters.
    if avgQrt == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["reference_period","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot_table(index="series_id",columns="reference_period",values=valuesForDF,aggfunc='first')
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        result.columns = modifyHeaders(result)
        return result
    # Checks if the user has had the data formatted by years.
    elif avgYear == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["year","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot(index="series_id",columns="year",values=valuesForDF)
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        # Merges the pivoted dataframe and the original one.
        result.columns = modifyHeaders(result)
        return result
    # Checks if the user has had the time formatted..
    elif timeForm == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["formatted_time","value","footnote_codes"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value","footnote_codes"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        if yearToDrop == 1:
            # Adds the year over year column to the dropped column list and the value list
            toDropFromDataframe.append("yearOverYear")
            valuesForDF.append("yearOverYear")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot(index="series_id",columns="formatted_time",values=valuesForDF)
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        result.columns = modifyHeaders(result)
        return result
    else:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["year","period","value","footnote_codes"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value","footnote_codes"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        if yearToDrop == 1:
            # Adds the year over year column to the dropped column list and the value list
            toDropFromDataframe.append("yearOverYear")
            valuesForDF.append("yearOverYear")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot_table(index="series_id",columns=["year","period"],values=valuesForDF,aggfunc='first')
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        result.columns = modifyHeaders(result)
        return result

# Modifies the row headers. Takes the column titles from the first row in the dataframe and renames the column index titles with the content. 
def changeRowHeaders(dataFrame):
    dfList = dataFrame.values.tolist()
    for i in range(0,len(dfList[0])):
        dataFrame = dataFrame.rename(columns = {i:dfList[0][i]})
    return dataFrame

# A function that encapsulates all the code that is needed to be run to produce formatted data.
def pcProcessing(inputArr):
    pcProxy["http"] = inputArr[len(inputArr)-1].get("http")
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current",inputArr[len(inputArr)-1])
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("pcCur",BLS_Request.getAllFilesInDirectory("pcCur")))
    return createCustomFormattedDataFrame(changeRowHeaders(readCSV(newPath)).drop([0]),inputArr)