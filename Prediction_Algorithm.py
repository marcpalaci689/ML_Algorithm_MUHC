import PatientData as PD
import PredictionMatrix as PM

def Predict(PatientID):
    # Set database here
    DB = 'devAEHRA'
    # get the patient's most recent planning history
    patient_latest_history = PD.patient_history(DB,PatientID)
    # Get predicted time until Ready for Treatment
    prediction = PM.Build_Prediction_Matrix(patient_latest_history,DB)

    # print results to shell
    for i in patient_latest_history:
        print(i)
    print('\n')
    print(prediction)
    
    return patient_latest_history , prediction 


