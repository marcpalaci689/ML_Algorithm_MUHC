import Data_Extraction_tool as ExDat
import numpy as np
import pandas as pd
import CountDays
import pickle
import datetime

progress = 0

# A function that returns a loading progressbar for printing to the console (serves no functional purpose)
def print_progress():
    global progress
    if progress < 21:
        print('['+'#'*progress+' '*(21-progress)+']  ',round((100/21)*progress,0),'%' + ' '*10,end='\r')
        progress+=1
    else:
        print('['+'#'*progress+' '*(21-progress)+']  '+'100%'+' '*10 +'\n')
        progress = 0


# This function takes an array of all due dates as well as an array of the priorities and returns an array of the Medically Ready date. For a P3 the Mediaclly
# Ready date is DueDate-14 days and for P4 the Medically Ready date is DuedDate-28 days
def get_MR_date(duedate,priority):
    MRdate = []
    for i in range(len(priority)):
        if priority[i] == 'SGAS_P3':
            if isinstance(duedate[i],datetime.datetime)==True:
                d = duedate[i] - datetime.timedelta(days=14)
            else:
                due = datetime.datetime.strptime(duedate[i], "%Y-%m-%d %H:%M:%S")
                d = due - datetime.timedelta(days=14)
        elif priority[i] == 'SGAS_P4':            
            if isinstance(duedate[i],datetime.datetime)==True:
                d = duedate[i] - datetime.timedelta(days=28)
            else:
                due = datetime.datetime.strptime(duedate[i], "%Y-%m-%d %H:%M:%S")
                d = due - datetime.timedelta(days=28)
        elif priority[i] == '0':
            d='0'
        MRdate.append(d)
    return MRdate   

# This function takes an array of beginnning dates and ancalculates the differences in business days (taking holidays into account) and ouputs an array
# of business days
def get_number_days(dfstart, dfend):
    # get the number of days not including the weekends
    A = [d for d in dfstart]
    B = [d for d in dfend]
    return CountDays.DayDifference(A,B,'QC')


# This function takes an array of the SGAS due date as well as an array of timestamps and returns the time left to the SGAS due date.
# The function does not give an exact number of days but rather groups the time left until SGAS due date as either 'Over 3 weeks', '3 weeks',
# '2 weeks', 1'week', '3 days', 'Passed Deadline'. From research I found that grouping time until deadline gives better results than actually
# returning simply the number of days left.
def days_to_deadline(deadline,timestamp):
    days_left = []
    for i in range(len(deadline)):
        if deadline[i] in ['0','NA',None]:
            days_left.append('Not Applicable')
        else:
            if isinstance(deadline[i],datetime.datetime)==False:
                deadline[i]=datetime.datetime.strptime(deadline[i], "%Y-%m-%d %H:%M:%S")
            d = deadline[i] - timestamp[i]
            if d.days<0:
                days_left.append('Passed Deadline')
            elif d.days<3:
                days_left.append('3 Days')
            elif d.days<7:
                days_left.append('1 Week')
            elif d.days<14:
                days_left.append('2 Weeks')
            elif d.days<21:
                days_left.append('3 Weeks')                 
            else:
                days_left.append('Over 3 Weeks')
    return days_left


# This function is the main callable function that builds the X and y matrices which are used in the Train_Algorithm.py script to train.
def build_matrices(DB):
    
    # from the Data_Extraction_tool get primary oncologists and merge with Ct-Sim data to create a list of primary oncologist with the corresponding patient ID
    primoncs=ExDat.get_primary_oncologist(DB)
    # trun primoncs into a dataframe
    primoncDataFrame = pd.DataFrame(primoncs)
    primoncDataFrame.columns = ['patientid','primaryoncologist']
    # If there are duplicate rows keep only the first one.
    primoncDataFrame.drop_duplicates(subset='patientid', keep='first', inplace=True)
    
    # from the Data_Extraction_tool get doctors and merge with MD data to create a list of doctors with the corresponding ID
    docs = ExDat.get_doctors(DB)
    # turn docs into a dataframe
    docDataFrame = pd.DataFrame(docs)
    docDataFrame.columns = ['patientid','timestamp','activitynum','doctor']

    # turn data of dictionary into a DataFrame to give it matrix-like properties for ease of manipulation
    dic = ExDat.ExtractData(DB)
    col_dict = {}


    print('***************************************************************')
    print('*                  Building Training Matrices                 *')
    print('***************************************************************\n')
    print('Calculating features from data...')
    print_progress()

    for key in dic:
        dic[key] = pd.DataFrame(dic[key])
        if key == 'DOSE':
            dic[key].columns = ['id','patientid','diagnosis','priority','alias','timestamp','sex','birthdate','activitynum','completiondate','duedate','dosimetryload']
        elif key == 'MR' :
            dic[key].columns = ['id','patientid','diagnosis','priority','alias','timestamp','sex','birthdate','activitynum','completiondate','duedate']
        else:
            dic[key].columns = ['id','patientid','diagnosis','priority','alias','timestamp','sex','birthdate','activitynum','completiondate','duedate']
        # Filter out when sex is labeled as 'Unkown'
        dic[key] = dic[key][dic[key].sex != 'Unknown']
        print_progress()

    # combine data from each step with the data from RFT in order to calculate time difference
    for key in dic:
        if key == 'RFT':
            print_progress()
            continue
        else:
            dic[key] = pd.merge(dic[key],dic['RFT'],how='inner', on='id',suffixes=('', '_y'))

            # make sure sex is constant
            dic[key] = dic[key][dic[key].sex == dic[key].sex_y]
     

            # delete duplicated columns and rename remaining ones
            dic[key].drop(['sex_y','priority_y','diagnosis_y','alias_y','birthdate_y','patientid_y','activitynum_y','completiondate_y','duedate_y'], axis=1, inplace=True)
            dic[key].rename(columns={'timestamp_y':'endtime'},inplace=True)
            dic[key].reset_index(drop=True, inplace=True)

            # Use birthdate column to calculate age and then rename column to age
            dic[key]['birthdate'] = pd.DatetimeIndex(dic[key]['endtime']).year - pd.DatetimeIndex(dic[key]['birthdate']).year
            dic[key].rename(columns={'birthdate' : 'age'}, inplace=True)

            # calculate days between RFT and task creation date (target values)
            dic[key]['timediff'] = get_number_days(dic[key]['timestamp'],dic[key]['endtime'])

            # remove rows that have negative day difference
            dic[key] = dic[key][dic[key].timediff > 0]
            dic[key].reset_index(drop=True, inplace=True)

            # add the primary oncologist to the CT and MR data by joining primoncDataFrame and dic['CT'] (and dic['MR']) on the patientID
            if key == 'CT' or key == 'MR':
                dic[key] = pd.merge(dic[key], primoncDataFrame, how='inner',on='patientid')
                dic[key].reset_index(drop=True, inplace=True)
            
            # add the MD contour tasked doctor by joining docDataFrame and dic['MD'] on 'patientid','timestamp' and 'activitynum'
            if key == 'MD':
                dic[key] = pd.merge(dic[key],docDataFrame, how='inner', on=['patientid','timestamp','activitynum'])
                dic[key].drop(['patientid','alias','endtime','activitynum','completiondate'], axis=1, inplace=True)
                # since there may be duplicates (MD contour gets tasked to multiple doctors), keep only the first doctor that it got tasked to
                dic[key].drop_duplicates(subset='id',keep='first', inplace=True)
                dic[key].reset_index(drop=True, inplace=True)
                
            
            if key == 'DOSE':
                dic[key].drop_duplicates(subset='id', keep='first', inplace=True)
                dic[key].reset_index(drop=True, inplace=True)

            if key in ['MR','CT','PRES','PHYS']:
                dic[key].drop(['activitynum','alias','completiondate','patientid','endtime'], axis=1, inplace=True)
            print_progress()
  

    # add days left until deadline as a feature by using the days_to_deadline function defined above
    for key in dic:
        if key in ['MR','RFT']:
            print_progress()
            continue
        deadline = list(dic[key]['duedate'])
        timestamp = list(dic[key]['timestamp'])
        days_left = days_to_deadline(deadline,timestamp)
        dic[key]['weekstodeadline'] = days_left
        print_progress()

    for key in dic:
        if key == 'RFT':
            continue
        print('Building ' + key + ' Sparse Matrix... ', end='')
        # create sparse matrices while keeping track of which column is what. Note that the colsfordummy array defines exactly
        # what features are used for each model
        if key == 'CT':
            colsfordummy = ['diagnosis','priority','primaryoncologist','sex','weekstodeadline']
        elif key == 'MD':
            colsfordummy = ['diagnosis','priority','doctor','sex','weekstodeadline']
        elif key == 'DOSE':
            colsfordummy = ['diagnosis','priority','sex','weekstodeadline','dosimetryload']
        elif key in ['PRES','PHYS']:
            colsfordummy = ['diagnosis','priority','sex','weekstodeadline']
        else:
            colsfordummy = ['diagnosis','priority','primaryoncologist','sex']
 
        tempDat = dic[key]['age'].reshape(len(dic[key]['age']), 1)
        
        # initiate a column list that will keep track of the columns for each sparse matrix (this is important for prediction purposes, 
        # since the prediction X matrix will have to have the exact same column configuration than the training X matrix)
        column = []

        # 
        for col in colsfordummy:
            # This will stack all the columns
            column  = np.hstack((column,pd.get_dummies(dic[key][col]).columns))
            # This will stack the dummy matrices
            tempDat = np.hstack((tempDat,pd.get_dummies(dic[key][col])))
        # Finally once all the dummy matrices are stacked (which is actually the X matrix), stack the 'timediff' columns (also known as the y matrix)
        tempDat = np.hstack((tempDat,dic[key]['timediff'].reshape(len(dic[key]['timediff']), 1)))
        
        # replace the dictionary data with the sparse matrix equivalent
        dic[key] = tempDat
        # once the columns have been stacked, save that column list into a dictionary
        col_dict[key] = column
        print('Done'+' '*5) 
    
    return dic, col_dict
