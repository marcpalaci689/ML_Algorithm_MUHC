import PatientData as PD
import PredictionMatrix as PM
import time
import math
import holidays
from datetime import timedelta as td
import datetime

# record the present time that the prediction is being made
present_time =datetime.datetime.now() 

# function that takes a prediction (as a float type number) as well as a starting date and returns the prediction as an actual date
# (taking into account holidays and weekends)
def return_date(start,prediction):
    Province = 'QC'
    done = False

    # if prediction is less than 1 day... then just calculate the amount of minutes and add to the start date
    if prediction < 1:
        minutes_left = round(prediction*1440,0)
        prediction_date = start+td(minutes=minutes_left)
        # If it happens to land on a weekend or holiday, just add 1 day until you reach a business day
        while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
            prediction_date = prediction_date + td(days=1)

    #if prediction is longer than 1 day do this:
    else:
        #First seperate the prediction into whole days and minutes
        prediction_date = start
        pred_days = math.floor(prediction)
        minutes_left = (prediction-pred_days)*1440
        
        # add the whole days to the start date
        for i in range(pred_days):
            prediction_date = prediction_date + td(days=1)
            # if the date lands on a holiday or weekend, keep adding 1 day until it lands on a business day
            while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
                prediction_date = prediction_date + td(days=1)
        
        #Now proceed to add the minutes left    
        prediction_date = prediction_date+td(minutes=minutes_left)
        # Once more if it lands on a holiday or weekend, keep adding 1 day until it lands on a business day
        while prediction_date.isoweekday() in [6,7] or prediction_date in holidays.Canada(prov=Province):
            prediction_date = prediction_date + td(days=1)

    return prediction_date

def Predict(PatientID):
    # Set database here
    DB = 'devAEHRA'

    # get the patient's most recent treatment planning history by calling the PatientData module which returns a chronological 
    # list of the tasks and appointments
    patient_latest_history = PD.patient_history(DB,PatientID)

    # check whether or not the patient has indeed had an appointment or a task yet (by checking that the list has at least one element in it)
    # Return a prediction of 0 if the patient has no tasks or appointments yet and raise a failed flag as well as a reason flag
    if len(patient_latest_history)<1 :
        prediction = 0   
        history={'response':'Failed','Reason':'This patient does not have any appointments or tasks to display'}


    #If the patient has a valid history, return a success flag, his/her history, and the prediction date
    else:
        history = {'response':'Success'}
        history['steps']=[]

        # Get predicted time until Ready for Treatment by calling the PredictionMatrix module.
        prediction = PM.Build_Prediction_Matrix(patient_latest_history,DB)
        
        # Now return the patient's latest history. The 'Medically Ready' stage was only used to improve the algorithm, however it is not 
        # required anymore, so do not include it in the history.
        for i in patient_latest_history:
            if i[3]=='MEDICALLY READY':
                continue
            else: 
                datetime=i[4] #keep track of the most recent creation date which will be used in conjunction with the prediction time to return the prediction date
                
                # The following if/else statement is only used to distinguish between tasks and appointments (useful for the front-end of the app)
                if i[3] == 'Ct-Sim':
                    date = i[4].strftime("%Y-%m-%d %H:%M:%S")
                    history['steps'].append({'cancer': i[1], 'priority':i[2], 'stage':i[3], 'appointment/task': 'appointment', 'AriaSerNum':i[11], 'date':date})
                else:
                    date = i[4].strftime("%Y-%m-%d %H:%M:%S")
                    history['steps'].append({'cancer': i[1], 'priority':i[2], 'stage':i[3], 'appointment/task': 'task', 'AriaSerNum':i[11], 'date':date})
                 
        # If patient already completed treatment planning, return the date that they were Ready for Treatment. Else return the predicted date.
        if prediction==0:
            history['prediction'] = history['steps'][-1]['date']
        else:      
            date_predicted = return_date(datetime,prediction)
            
            # compare the predicted date with the current date to see if the prediction date has already been surpassed. If it has, then return the SGAS due date instead 
            # of the prediction
            if present_time>=date_predicted :
                history['prediction'] = i[10].strftime("%Y-%m-%d %H:%M:%S") #returning SGAS duedate as a string
            else:
                history['prediction']= date_predicted.strftime("%Y-%m-%d %H:%M:%S") #returning the prediction date as a string

        history['SGASDueDate'] = i[10].strftime("%Y-%m-%d %H:%M:%S")

    # Print the dictionary of results so that it can be parsed in JavaScript             
    print(history)
  