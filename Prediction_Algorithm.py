import PatientData as PD
import PredictionMatrix as PM
import time
import math
import holidays
from datetime import timedelta as td
import datetime

### function that takes a prediction (as a float type number) and returns the prediction as a string date
def return_date(start,prediction):
    Province = 'QC'
    done = False
    if prediction < 1:
        minutes_left = round(prediction*1440,0)
        prediction_date = start+td(minutes=minutes_left)
        while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
            prediction_date = prediction_date + td(days=1)
    else:
        prediction_date = start
        pred_days = math.floor(prediction)
        minutes_left = (prediction-pred_days)*1440
        
        for i in range(pred_days):
            prediction_date = prediction_date + td(days=1)
            while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
                prediction_date = prediction_date + td(days=1)
            
        prediction_date = prediction_date+td(minutes=minutes_left)
        while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
            prediction_date = prediction_date + td(days=1)

        prediction_date=prediction_date.strftime("%Y-%m-%d %H:%M:%S")
    
    return prediction_date

def Predict(PatientID):
    # Set database here
    DB = 'devAEHRA'
    # get the patient's most recent planning history
    patient_latest_history = PD.patient_history(DB,PatientID)
    # Get predicted time until Ready for Treatment
    prediction = PM.Build_Prediction_Matrix(patient_latest_history,DB)

    history = []
    for i in patient_latest_history:
        if i[3]=='MEDICALLY READY':
            continue
        else: 
            datetime=i[4] #This is only here to keep track of the most recent creation date which will be used in conjunction with the prediction time to return the prediction date
            date = i[4].strftime("%Y-%m-%d %H:%M:%S")
            history.append([i[1],i[2],i[3],i[11],date])
    
    # If patient already completed treatment planning, return 0. Else return the predicted date.
    if prediction==0:
        prediction_date = 0
    else:   
        prediction_date = return_date(datetime,prediction)

    for i in history:
        print(i)
    print('\n')
    print(prediction_date)


    return history , prediction_date 


