import Data_Extraction_tool as ExDat
import numpy as np
import pandas as pd
import CountDays
import pickle
import datetime

progress = 0

# A function that returns a loading progressbar for printing to the console
def print_progress():
    global progress
    if progress < 28:
        print('['+'#'*progress+' '*(28-progress)+']  ',round((100/28)*progress,0),'%' + ' '*20,end='\r')
        progress+=1
    else:
        print('['+'#'*progress+' '*(28-progress)+']  '+'100%'+' '*20 +'\n')
        progress = 0



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



def timesofar(dfstart, dfend, key):
    t = []
    timesofar = get_number_days(dfstart, dfend)
    if key == 'CT':
        extreme_limit = 0.1
        bottom_limit = 1
        top_limit = 2
    elif key == 'MD':
        extreme_limit = 0.2
        bottom_limit = 2
        top_limit = 4
    elif key == 'DOSE':
        extreme_limit = 0.3
        bottom_limit = 3
        top_limit = 5
    elif key == 'PRES':
        extreme_limit = 0.35
        bottom_limit = 3.25
        top_limit = 5.5
    elif key == 'PHYS':
        extreme_limit = 0.4
        bottom_limit = 3.5
        top_limit = 6
    for i in timesofar:
        if i == 'NA':
            t.append('NA')
            continue
        if i <= extreme_limit:
            t.append('extremely fast')
        elif i <= bottom_limit:
            t.append('very fast')
        elif i <= top_limit:
            t.append('fast')
        else:
            t.append('normal')
    return t

def get_number_days(dfstart, dfend):
    # get the number of days not including the weekends
    A = [d for d in dfstart]
    B = [d for d in dfend]
    return CountDays.DayDifference(A,B,'QC')


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


def build_matrices(DB):
    
    # get primary oncologists and merge with Ct-Sim data to create a list of primary oncologist with the corresponding patient ID
    primoncs=ExDat.get_primary_oncologist(DB)
    primoncDataFrame = pd.DataFrame(primoncs)
    primoncDataFrame.columns = ['patientid','primaryoncologist']
    primoncDataFrame.drop_duplicates(subset='patientid', keep='first', inplace=True)
    
    # get doctors and merge with MD data to create a list of doctors with the corresponding ID
    docs = ExDat.get_doctors(DB)
    docDataFrame = pd.DataFrame(docs)
    docDataFrame.columns = ['patientid','timestamp','activitynum','doctor']

    # turn data of dictionary into a DataFrame to give it matrix-like properties for ease of manipulation
    dic = ExDat.ExtractData(DB)
    col_dict = {}

    print('***********************************************************************************')
    print('*                            Building Training Matrices                           *')
    print('***********************************************************************************\n')
    print('Calculating features from data...')
    print_progress()

    for key in dic:
        dic[key] = pd.DataFrame(dic[key])
        if key == 'DOSE':
            dic[key].columns = ['id','patientid','diagnosis','priority','alias','timestamp','sex','birthdate','activitynum','completiondate','duedate','dosimetryload']
        elif key == 'MR' :
            dic[key].columns = ['id','patientid','diagnosis','priority','alias','timestamp','sex','birthdate','activitynum','completiondate','duedate','patientload']
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

            if key == 'CT' or key == 'MR':
                dic[key] = pd.merge(dic[key], primoncDataFrame, how='inner',on='patientid')
                dic[key].reset_index(drop=True, inplace=True)
            
            if key == 'MD':
                dic[key] = pd.merge(dic[key],docDataFrame, how='inner', on=['patientid','timestamp','activitynum'])
                dic[key].drop(['patientid','alias','endtime','activitynum','completiondate'], axis=1, inplace=True)
                dic[key].drop_duplicates(subset='id',keep='first', inplace=True)
                dic[key].reset_index(drop=True, inplace=True)
                
            
            if key == 'DOSE':
                #dic[key] = pd.merge(dic[key],doseDataFrame, how='inner', on=['patientid','timestamp','activitynum'])
                #dic[key].drop(['patientid','alias','endtime','activitynum','completiondate'], axis=1, inplace=True)
                dic[key].drop_duplicates(subset='id', keep='first', inplace=True)
                dic[key].reset_index(drop=True, inplace=True)

            if key in ['MR','CT','PRES','PHYS']:
                dic[key].drop(['activitynum','alias','completiondate','patientid','endtime'], axis=1, inplace=True)
            print_progress()

    #add the current speed of the planning process as a feature
    for key in dic:
        if key in ['MR','RFT']:
            print_progress()
            continue
        #dic[key] = pd.merge(dic[key],dic['MR'], how='inner' , on='id' , suffixes=('','_y'))
        #dic[key].reset_index(drop=True, inplace=True)
        #dic[key].drop(['sex_y','priority_y','diagnosis_y'], axis=1, inplace=True)
        dic[key]['timestamp_y'] = get_MR_date(dic[key]['duedate'],dic[key]['priority'])
        dic[key]['timesofar'] = timesofar(dic[key]['timestamp_y'],dic[key]['timestamp'],key)
        dic[key].drop('timestamp_y',axis=1,inplace=True)
        print_progress()     

    # add weeks left until deadline as a feature
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
        # create sparse matrices while keeping track of which column is what
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

        column = []


        
        for col in colsfordummy:
            column  = np.hstack((column,pd.get_dummies(dic[key][col]).columns))
            tempDat = np.hstack((tempDat,pd.get_dummies(dic[key][col])))
        tempDat = np.hstack((tempDat,dic[key]['timediff'].reshape(len(dic[key]['timediff']), 1)))
        dic[key] = tempDat
        col_dict[key] = column
        print('Done') 
    
    return dic, col_dict
