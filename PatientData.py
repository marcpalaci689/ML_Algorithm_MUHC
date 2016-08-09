import mysql.connector
import time
import datetime
from datetime import timedelta as td
import holidays
import pickle
import PredictionMatrix as PM


### This script is used to look up the patients most recent data and return their latest treatment course. If the patient happens to have a boost treatment or something of the like
### then it will only display the first treatment course. Some patients will have a long treatment course with multiple Ready for Treatment tasks and it is impossible
### to know what or why this happened. Hence we cannot give predictions for these cases.

def get_cancer(DB):
    can_cnx = mysql.connector.connect(user='marcpalaci689',password='crapiola689', database=DB)
    can_cur = can_cnx.cursor()

    can_cur.execute(''' SELECT DiagnosisCode, AliasName FROM diagnosistranslation  ''')

    can = can_cur.fetchall()
    diagnosiscode=[]
    cancer = []
    for i in can:
        diagnosiscode.append(i[0].rstrip())
        cancer.append(i[1].rstrip())
    return diagnosiscode,cancer



def patient_history(DB,Patient):
    
    # Query Database to get the patients most recent treatment planning history
    
    cnx=mysql.connector.connect(user='marcpalaci689',password='crapiola689', database=DB)
    cur=cnx.cursor()
    start=time.time()
    cur.execute(''' SELECT  patient.PatientSerNum, diagnosis.DiagnosisCode, priority.PriorityCode, alias.AliasName, appointment.ScheduledStartTime, priority.CreationDate,
    patient.Sex, patient.DateOfBirth,appointment.ActivityInstanceAriaSer, appointment.ScheduledEndTime, priority.DueDateTime
    FROM appointment JOIN patient ON appointment.PatientSerNum=patient.PatientSerNum 
    JOIN diagnosis ON appointment.DiagnosisSerNum = diagnosis.DiagnosisSerNum
    JOIN priority ON appointment.PrioritySerNum = priority.PrioritySerNum 
    JOIN alias ON appointment.AliasSerNum = alias.AliasSerNum 
    WHERE appointment.PatientSerNum = %s AND appointment.Status!='Cancelled' AND appointment.State!='Deleted' AND appointment.AliasSerNum!=6
    UNION ALL SELECT
    patient.PatientSerNum, diagnosis.DiagnosisCode, priority.PriorityCode, alias.AliasName, task.CreationDate, priority.CreationDate, patient.Sex,patient.DateOfBirth,
    task.ActivityInstanceAriaSer, task.CompletionDate, priority.DueDateTime
    FROM task JOIN patient ON task.PatientSerNum=patient.PatientSerNum 
    JOIN diagnosis ON task.DiagnosisSerNum = diagnosis.DiagnosisSerNum
    JOIN priority ON task.PrioritySerNum = priority.PrioritySerNum 
    JOIN alias ON task.AliasSerNum = alias.AliasSerNum 
    WHERE task.PatientSerNum = %s AND task.Status != 'Cancelled' AND task.State != 'Deleted' AND task.AliasSerNum!=6
    UNION ALL SELECT
    patient.PatientSerNum, diagnosis.DiagnosisCode, priority.PriorityCode, alias.AliasName, task.DueDateTime, priority.CreationDate, patient.Sex,patient.DateOfBirth,
    task.ActivityInstanceAriaSer, task.CompletionDate, priority.DueDateTime
    FROM task JOIN patient ON task.PatientSerNum=patient.PatientSerNum 
    JOIN diagnosis ON task.DiagnosisSerNum = diagnosis.DiagnosisSerNum
    JOIN priority ON task.PrioritySerNum = priority.PrioritySerNum 
    JOIN alias ON task.AliasSerNum = alias.AliasSerNum
    WHERE task.AliasSerNum=6 AND task.Status != 'Cancelled' AND task.State != 'Deleted' AND task.PatientSerNum = %s''' , (Patient,Patient,Patient))

    data_1 = cur.fetchall()
    data_1 = list(data_1)

    # Add the no priority CT (if there is one)
    ct_cnx=mysql.connector.connect(user='marcpalaci689',password='crapiola689',database=DB)
    ct_cur = ct_cnx.cursor()

    ct_cur.execute('''SELECT  patient.PatientSerNum, diagnosis.DiagnosisCode, appointment.PrioritySerNum, alias.AliasName, appointment.ScheduledStartTime, appointment.PrioritySerNum,
    patient.Sex, patient.DateOfBirth,appointment.ActivityInstanceAriaSer, appointment.ScheduledEndTime, appointment.PrioritySerNum
    FROM appointment JOIN patient ON appointment.PatientSerNum=patient.PatientSerNum 
    JOIN diagnosis ON appointment.DiagnosisSerNum = diagnosis.DiagnosisSerNum
    JOIN alias ON appointment.AliasSerNum = alias.AliasSerNum 
    WHERE appointment.PatientSerNum = %s AND appointment.AliasSerNum = 3 AND appointment.Status != 'Cancelled' AND appointment.PrioritySerNum=0
    UNION ALL
    SELECT  patient.PatientSerNum, appointment.DiagnosisSerNum, appointment.PrioritySerNum, alias.AliasName, appointment.ScheduledStartTime, appointment.PrioritySerNum,
    patient.Sex, patient.DateOfBirth,appointment.ActivityInstanceAriaSer, appointment.ScheduledEndTime, appointment.PrioritySerNum
    FROM appointment JOIN patient ON appointment.PatientSerNum=patient.PatientSerNum 
    JOIN alias ON appointment.AliasSerNum = alias.AliasSerNum 
    WHERE appointment.PatientSerNum = %s AND appointment.AliasSerNum = 3 AND appointment.Status != 'Cancelled' AND appointment.PrioritySerNum=0 AND
    appointment.DiagnosisSerNum=0
    UNION ALL
    SELECT  patient.PatientSerNum, appointment.DiagnosisSerNum, priority.PriorityCode, alias.AliasName, appointment.ScheduledStartTime, priority.CreationDate,
    patient.Sex, patient.DateOfBirth,appointment.ActivityInstanceAriaSer, appointment.ScheduledEndTime, priority.DueDateTime
    FROM appointment JOIN patient ON appointment.PatientSerNum=patient.PatientSerNum 
    JOIN priority ON appointment.PrioritySerNum = priority.PrioritySerNum 
    JOIN alias ON appointment.AliasSerNum = alias.AliasSerNum 
    WHERE appointment.PatientSerNum = %s AND appointment.AliasSerNum = 3  AND appointment.Status != 'Cancelled' AND appointment.DiagnosisSerNum=0 ''', (Patient,Patient,Patient))

    ct_data=ct_cur.fetchall()
    ct_data = list(ct_data)

    #Add non-priority CTs to the data 
    data = data_1 + ct_data

    # Filter out unnecessary aliases and priorities. Also set the maximum time limit to 150 days
    current_date = datetime.datetime.today()
    beginning_date = current_date - datetime.timedelta(days=150)
    aliases = ['Ct-Sim','READY FOR MD CONTOUR','READY FOR DOSE CALCULATION','READY FOR PHYSICS QA','READY FOR TREATMENT','End of Treament Note Task']
    ordered_data = []
    for i in data:
        if (i[2] in ['SGAS_P3', 'SGAS_P4']) and (i[3] in aliases) and (i[4]>beginning_date):
            ordered_data.append(i)
    ordered_data.sort(key=lambda x : x[4])

    # Filter out consecutive duplicates... keep only first instance
    unique_data = []
    last_alias='NONE'
    last_priority='NONE'
    for i in ordered_data:
        current_alias = i[3]
        current_priority = i[2]
        if current_alias != last_alias:
            unique_data.append(i)
            last_alias=current_alias
            last_priority=current_priority
        elif current_alias == last_alias and current_priority!=last_priority:
            del unique_data[-1]
            unique_data.append(i)
            last_alias=current_alias
            last_priority=current_priority
        else:
            continue


    MR_data = []
    for i in unique_data:
        if i[1]=='0' and i[2]!='0':
            if i[2] == 'SGAS_P3':
                due=datetime.datetime.strptime(i[10], "%Y-%m-%d %H:%M:%S")
                creation = due-datetime.timedelta(days=14)
            else:
                due=datetime.datetime.strptime(i[10], "%Y-%m-%d %H:%M:%S")
                creation = due-datetime.timedelta(days=28) 
        elif i[2] == '0':
            creation='0'
        elif i[2] == 'SGAS_P3':
            diff = 14
            due=i[10]
            creation = due-datetime.timedelta(days=diff)
        elif i[2] == 'SGAS_P4':
            diff = 28
            due=i[10]
            creation = i[10]-datetime.timedelta(days=diff)
        MR_data.append((i[0],i[1],i[2],i[3],i[4],creation)+i[6:])

    complete_data = []
    # set a flag to know whether 'Medically Ready' task has been established already for the patient
    flag = False
    if MR_data[0][5] < MR_data[0][4]:
        complete_data.append((MR_data[0][0],MR_data[0][1],MR_data[0][2],'MEDICALLY READY',MR_data[0][5]) + MR_data[0][5:])
        flag = True
    for i in MR_data:
        if len(complete_data)==0:
            pass
        else:
            if i[5] != complete_data[len(complete_data)-1][5]:
                flag=False
        if i[5] == None or i[5]=='0':
            complete_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
            continue
        if i[5]<i[4] and flag==False:
            complete_data.append((i[0],i[1],i[2],'MEDICALLY READY',i[5]) + i[5:])
            complete_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
            flag = True
        else:
            complete_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])

  
    # Keep only most recent treatment course by using End of Treatment Note Task as indicator
    last_course = []
    last_course.append(complete_data[len(complete_data)-1])
    del(complete_data[-1])
    for i in reversed(complete_data):
        if i[3] == 'End of Treament Note Task':
            break
        else:
            last_course.append(i)
    last_course = reversed(last_course)        
  
    
    
    # Determine whether a prediction should be given for this patient or not
    # My logic is that if the  most recent course of treatment has a 'READY FOR TREATMENT' task, then no prediction should be made
    predict = True
    history = []
    for i in last_course:
        history.append(list(i))
        if i[3] == 'READY FOR TREATMENT':
            predict = False
            break


    diagnosiscode,cancer = get_cancer(DB)
    history_withDiagnosis = []
    for i in history:
        try:
            cantype = cancer[diagnosiscode.index(i[1])]
            history_withDiagnosis.append(i[:1] + [cantype] + i[2:])
        except ValueError:
            history_withDiagnosisappend(i[:1] + ['Other'] + i[2:])
    
    return history_withDiagnosis


    # If a prediction is expected, build matrix and return prediction with current treatment history. If not, then just return the
    # the current history

