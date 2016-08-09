import numpy as np
import pandas as pd
import pickle
import datetime
import mysql.connector
import time
from dateutil.relativedelta import relativedelta
import BuildInputMatrix as BIM
import sklearn
import warnings

# function that will build the prediction matrix
def Build_Prediction_Matrix(data,DB):
    # We only need the last step of the patients data to make a prediction, so only keep that step. Also turn into an array for ease of manipulation
    prediction_step = np.array(data[len(data)-1]).T              
    # Use birthdate to calculate age and then rename column to age
    age = relativedelta(prediction_step[4], prediction_step[7]).years
    # record the stage from which prediction is needed
    alias = prediction_step[3]
    # Find the time left until SGAS Due Date by calling days_to_deadline function
    weekstodeadline = days_to_deadline(prediction_step[10], prediction_step[4])
  
    
    # Now add stage specific features such as primary oncologist (for Ct-Sim), MD contour doctor (for MD Contour) and Dosimetry load (For Dose Calculation)
    # if there are no stage specific features set the feature variable to 'NoFeature'
    if alias == 'Ct-Sim':
        feature = get_primary_oncologist(prediction_step[0],DB)
        model = 'CT.pkl'
        col = 'CT'
    elif alias == 'READY FOR MD CONTOUR':
        feature = get_MDdoctor(prediction_step[8],DB)
        model = 'MD.pkl'
        col = 'MD'
    elif alias == 'READY FOR DOSE CALCULATION':
        feature = dosimetry(DB,prediction_step[4])
        model = 'DOSE.pkl'
        col = 'DOSE'
    elif alias == 'MEDICALLY READY':
        feature = 'NoFeature'
        model = 'MR.pkl'
        col = 'MR'
    elif alias == 'READY FOR PHYSICS QA':
        feature = 'NoFeature'
        model = 'PHYS.pkl'
        col = 'PHYS'    
    else:
        feature = 'NoFeature'


    # concatenate all age, weekstodeadline, and stage specific feature to step_prediction 
    prediction_step = np.concatenate(([age,weekstodeadline,feature],prediction_step))

    # Build the sparse matrix and predict for that matrix. If the patient already reached Ready For Treatment, simply return 0
    if alias != 'READY FOR TREATMENT':
        model = pickle.load(open(model,'rb'))
        columndictionary = pickle.load(open('columndictionary.pkl','rb'))
        column = columndictionary[col]
        x = BIM.CreateInputMatrix(prediction_step,column)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        prediction = model.predict(x)
        prediction = prediction[0]
    else:
        prediction = 0
    
    return prediction

# This function will extract all the Dose Calculations currently active in order to calculate the dosimetry load
def dosimetry(DB,creation_date):
    start = time.time()
    # set a limit for how far back in time to look. This limit will be set as 14 days. This limit is to improve efficiency, also completion dates are sometimes absent
    # so setting a limit reduces errors.
    limit = creation_date - datetime.timedelta(days=14)
    d_cnx=mysql.connector.connect(user='marcpalaci689',password='crapiola689', database=DB)
    d_cur=d_cnx.cursor()
    ### Fetch dosimetry data from database

    d_cur.execute('''SELECT Task.PatientSerNum, Task.AliasSerNum, Task.Status, Task.CreationDate, Task.CompletionDate FROM Task
    WHERE Task.AliasSerNum IN (22,19) AND Task.Status != 'Cancelled' AND Task.State != 'Deleted' AND Task.CreationDate > %s  ''', (limit,))
 
    data=d_cur.fetchall()
    # to reduce errors, delete all patient that have been tasked READY FOR TREATMENT before the Dose Calculation timestamp. This ensures that dosimetry
    # calculations that are already done are not accidentially counted in the dosimetry load.
    new_data =[]
    delete_patients=[]
    for i in data:
        if i[1] == 19:
            delete_patients.append(i[0])
    for i in data:
        if i[0] not in delete_patients:
            new_data.append(list(i))
    end = time.time()
    # Count all the dosimetry calculations that were open or in progress during the patients Dose Calculation creation date
    load=0
    for i in new_data:
        if i[4] != None:
            if creation_date >= i[3] and creation_date <= i[4]:
                load+=1
        if creation_date >= i[3] and i[4] == None:
            if i[2] in ['Open','In Progress']:
                load+=1
                
    return str(load)
#  This function will obtain the first primary oncologist assigned to the patient
def get_primary_oncologist(patient,DB):
    onc_cnx = mysql.connector.connect(user='marcpalaci689',password='crapiola689', database=DB)
    onc_cur = onc_cnx.cursor()

    onc_cur.execute(''' SELECT Patient.PatientSerNum, Doctor.LastName
    FROM Patient JOIN PatientDoctor on Patient.PatientSerNum = PatientDoctor.PatientSerNum
    JOIN Doctor on Doctor.DoctorSerNum = PatientDoctor.DoctorSerNum
    WHERE PatientDoctor.OncologistFlag = 1 AND PatientDoctor.PrimaryFlag = 1 AND Patient.PatientSerNum = %s  ''' , (patient,))

    primonc = onc_cur.fetchall()
    primonc = primonc[len(primonc)-1][1]
    return primonc

# This function will query the database and obtain the doctor assigned to the MD contour of each patient 
def get_MDdoctor(activitysernum,DB):
    doc_cnx = mysql.connector.connect(user='marcpalaci689',password='crapiola689', database=DB)
    doc_cur = doc_cnx.cursor()

    doc_cur.execute(''' SELECT Task.PatientSerNum, Task.CreationDate,Task.ActivityInstanceAriaSer, Resource.ResourceName
    FROM Task JOIN Attendee ON Task.ActivityInstanceAriaSer = Attendee.ActivityInstanceAriaSer
    JOIN Resource ON Attendee.ResourceSerNum = Resource.ResourceSerNum
    WHERE Task.AliasSerNum = 8 AND Resource.ResourceType = 'Doctor' AND Task.ActivityInstanceAriaSer = %s  ''' ,(activitysernum,))

    docs = doc_cur.fetchall()
    list(docs)
    docs.sort(key=lambda x: x[1])
    doctor=docs[0][3]
    return doctor

def days_to_deadline(deadline,timestamp):
    if deadline in ['0','NA',None]:
        days_left='Not Applicable'
    else:
        if isinstance(deadline,datetime.datetime)==False:
            deadline=datetime.datetime.strptime(deadline, "%Y-%m-%d %H:%M:%S")
        d = deadline - timestamp
        if d.days<0:
            days_left = 'Passed Deadline'
        elif d.days<3:
            days_left = '3 Days'
        elif d.days<7:
            days_left = '1 Week'
        elif d.days<14:
            days_left = '2 Weeks'
        elif d.days<21:
            days_left = '3 Weeks'                 
        else:
            days_left = 'Over 3 Weeks'
    return days_left

