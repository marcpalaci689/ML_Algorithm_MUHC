'''
This script will query the devAEHRA database and extract a list of tuples containing all relevant tasks and appointments for each patient.
Every new task/appointment will be a new tuple. The list is ordered first py patient number and then by date so that you can visualize
the sequence of each patient chronologically. For this script to work the first you should keep the same exact column structure as in
my queries. Any addictional needed information can be added afterwards (for example if you want to add another column it should work fine.
'''


import datetime
import time
import mysql.connector
from datetime import timedelta as td
import holidays
import pickle


### This function returns the number of business days (to the fraction) between two datetimes
def DayDifference(start_date,end_date,Province):
    start_time=time.time()
    d2=end_date
    d1=start_date
    count=(d2.hour-d1.hour)/24 + (d2.minute-d1.minute)/1440

    if d1.date() == d2.date():
        pass 
    elif d1.date()>d2.date():
        delta= d1.date()-d2.date()
        for i in range(delta.days):
            day=d2+td(days=(i+1))
            if not (day.isoweekday() in [6,7]) and not (day in holidays.Canada(prov=Province)):
                count-=1       
    else:
        delta= d2.date()-d1.date()
        for i in range(delta.days):
            day=d1+td(days=(i+1))
            if not (day.isoweekday() in [6,7]) and not (day in holidays.Canada(prov='QC')):
                count+=1
            
    end_time = time.time()
    return count

#  This function will obtain the first primary oncologist assigned to the patient
def get_primary_oncologist(DB):
    onc_cnx = mysql.connector.connect(user='root',password='service', database=DB)
    onc_cur = onc_cnx.cursor()

    onc_cur.execute(''' SELECT Patient.PatientSerNum, Doctor.LastName
    FROM Patient JOIN Patientdoctor on Patient.PatientSerNum = Patientdoctor.PatientSerNum
    JOIN Doctor on Doctor.DoctorSerNum = Patientdoctor.DoctorSerNum
    WHERE Patientdoctor.OncologistFlag = 1 AND Patientdoctor.PrimaryFlag = 1 ''')

    primonc = onc_cur.fetchall()
    return primonc

            
# This function will query the database and obtain the dosimetrist assigned to the Dose Calculation of each patient 
def get_dosimetrist(DB):
    dos_cnx = mysql.connector.connect(user='root',password='service', database=DB)
    dos_cur = dos_cnx.cursor()

    dos_cur.execute(''' SELECT Task.PatientSerNum, Task.CreationDate,Task.ActivityInstanceAriaSer, Resource.ResourceName
    FROM Task JOIN Attendee ON Task.ActivityInstanceAriaSer = Attendee.ActivityInstanceAriaSer
    JOIN Resource ON Attendee.ResourceSerNum = Resource.ResourceSerNum
    WHERE Task.AliasSerNum = 22 AND Resource.ResourceType = 'Staff' AND CreationDate > '2012-01-01 00:00:00' ''')

    dose = dos_cur.fetchall()
    dosimetrists = []
    for i in dose:
        dosimetrists.append(i)
    return dosimetrists



# This function will query the database and obtain the doctor assigned to the MD contour of each patient 
def get_doctors(DB):
    doc_cnx = mysql.connector.connect(user='root',password='service', database=DB)
    doc_cur = doc_cnx.cursor()

    doc_cur.execute(''' SELECT Task.PatientSerNum, Task.CreationDate,Task.ActivityInstanceAriaSer, Resource.ResourceName
    FROM Task JOIN Attendee ON Task.ActivityInstanceAriaSer = Attendee.ActivityInstanceAriaSer
    JOIN Resource ON Attendee.ResourceSerNum = Resource.ResourceSerNum
    WHERE Task.AliasSerNum = 8 AND Resource.ResourceType = 'Doctor' AND CreationDate > '2012-01-01 00:00:00'  ''')

    docs = doc_cur.fetchall()
    doctors = []
    for i in docs:
        doctors.append(i)
    return doctors

def get_cancer(DB):
    can_cnx = mysql.connector.connect(user='root',password='service', database=DB)
    can_cur = can_cnx.cursor()

    can_cur.execute(''' SELECT DiagnosisCode, AliasName FROM DiagnosisTranslation  ''')

    can = can_cur.fetchall()
    diagnosiscode=[]
    cancer = []
    for i in can:
        diagnosiscode.append(i[0].rstrip())
        cancer.append(i[1].rstrip())
    return diagnosiscode,cancer

                

# This function will query the database and obtain the complete treatment history of each patient
def query_database(DB):
    cnx=mysql.connector.connect(user='root',password='service', database=DB)
    cur=cnx.cursor()
    ### Fetch data from database
    print('----------------- querying database ------------------')
    start=time.time()
    cur.execute(''' SELECT * FROM
    (SELECT  Patient.PatientSerNum, Diagnosis.DiagnosisCode, Priority.PriorityCode, Alias.AliasName, Appointment.ScheduledStartTime, Priority.CreationDate,
    Patient.Sex, Patient.DateOfBirth,Appointment.ActivityInstanceAriaSer, Appointment.ScheduledEndTime, Priority.DueDateTime
    FROM Appointment JOIN Patient ON Appointment.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Appointment.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Priority ON Appointment.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Appointment.AliasSerNum = Alias.AliasSerNum 
    WHERE Appointment.AliasSerNum = 3 AND Appointment.Status!='Cancelled' AND Appointment.State!='Deleted' 
    UNION ALL SELECT
    Patient.PatientSerNum, Diagnosis.DiagnosisCode, Priority.PriorityCode, Alias.AliasName, Task.CreationDate, Priority.CreationDate, Patient.Sex,Patient.DateOfBirth,
    Task.ActivityInstanceAriaSer, Task.CompletionDate, Priority.DueDateTime
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Task.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Priority ON Task.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE (Task.AliasSerNum = 8 OR Task.AliasSerNum=22 OR Task.AliasSerNum=18 OR Task.AliasSerNum=19) AND Task.Status != 'Cancelled' AND Task.State != 'Deleted'
    UNION ALL SELECT
    Patient.PatientSerNum, Diagnosis.DiagnosisCode, Priority.PriorityCode, Alias.AliasName, Task.DueDateTime, Priority.CreationDate, Patient.Sex,Patient.DateOfBirth,
    Task.ActivityInstanceAriaSer, Task.CompletionDate, Priority.DueDateTime
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Task.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Priority ON Task.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum=6
    UNION ALL SELECT
    Patient.PatientSerNum, Diagnosis.DiagnosisCode, Priority.PriorityCode, Alias.AliasName, Document.ApprovedTimeStamp, Priority.CreationDate,
    Patient.Sex,Patient.DateOfBirth, Document.DocumentSerNum, Document.DateOfService, Priority.DueDateTime
    FROM Document JOIN Patient ON Document.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Document.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Priority ON Document.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Document.AliasSerNum = Alias.AliasSerNum 
    WHERE (Document.AliasSerNum = 20 OR Document.AliasSerNum=21))dum
    WHERE (PriorityCode = 'SGAS_P4' OR PriorityCode='SGAS_P3') AND ScheduledStartTime > '2012-01-01 00:00:00'   
    ORDER BY PatientSerNum, ScheduledStartTime ''')

     
    data=cur.fetchall()
    end = time.time()
    print('--------------- done in %.3f seconds ---------------\n' %(end-start))
    data_file = 'data.pkl'
    file = open(data_file,'wb')
    pickle.dump(data,file)
    file.close()
    return data

def add_nopriorityCT(DB,data):
    new_data=[]

    ct_cnx=mysql.connector.connect(user='root',password='service', database=DB)
    ct_cur = ct_cnx.cursor()

    ct_cur.execute('''SELECT  Patient.PatientSerNum, Diagnosis.DiagnosisCode, Appointment.PrioritySerNum, Alias.AliasName, Appointment.ScheduledStartTime, Appointment.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Appointment.ActivityInstanceAriaSer, Appointment.ScheduledEndTime, Appointment.PrioritySerNum
    FROM Appointment JOIN Patient ON Appointment.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Appointment.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Alias ON Appointment.AliasSerNum = Alias.AliasSerNum 
    WHERE Appointment.AliasSerNum = 3 AND Appointment.Status != 'Cancelled' AND ScheduledStartTime > '2012-01-01 00:00:00' AND Appointment.PrioritySerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Appointment.DiagnosisSerNum, Appointment.PrioritySerNum, Alias.AliasName, Appointment.ScheduledStartTime, Appointment.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Appointment.ActivityInstanceAriaSer, Appointment.ScheduledEndTime, Appointment.PrioritySerNum
    FROM Appointment JOIN Patient ON Appointment.PatientSerNum=Patient.PatientSerNum 
    JOIN Alias ON Appointment.AliasSerNum = Alias.AliasSerNum 
    WHERE Appointment.AliasSerNum = 3 AND Appointment.Status != 'Cancelled' AND ScheduledStartTime > '2012-01-01 00:00:00' AND Appointment.PrioritySerNum=0 AND
    Appointment.DiagnosisSerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Appointment.DiagnosisSerNum, Priority.PriorityCode, Alias.AliasName, Appointment.ScheduledStartTime, Priority.CreationDate,
    Patient.Sex, Patient.DateOfBirth,Appointment.ActivityInstanceAriaSer, Appointment.ScheduledEndTime, Priority.DueDateTime
    FROM Appointment JOIN Patient ON Appointment.PatientSerNum=Patient.PatientSerNum 
    JOIN Priority ON Appointment.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Appointment.AliasSerNum = Alias.AliasSerNum 
    WHERE Appointment.AliasSerNum = 3  AND Appointment.Status != 'Cancelled' AND Appointment.DiagnosisSerNum=0  AND
    ScheduledStartTime > '2012-01-01 00:00:00' AND (Priority.PriorityCode='SGAS_P3' OR Priority.PriorityCode='SGAS_P4') ''')

    ct_data=ct_cur.fetchall()
    new_data = data+ct_data
    new_data.sort(key=lambda x:[x[0],x[4]])
    return new_data

def add_nopriorityTasks(DB,data):
    new_data=[]

    task_cnx=mysql.connector.connect(user='root',password='service', database=DB)
    task_cur = task_cnx.cursor()

    task_cur.execute('''SELECT  Patient.PatientSerNum, Diagnosis.DiagnosisCode, Task.PrioritySerNum, Alias.AliasName, Task.CreationDate, Task.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Task.PrioritySerNum
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Task.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum IN (8, 18, 19,22) AND Task.Status != 'Cancelled'  AND Task.State != 'Deleted' AND Task.CreationDate > '2012-01-00 00:00:00' AND Task.PrioritySerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Task.DiagnosisSerNum, Task.PrioritySerNum, Alias.AliasName, Task.CreationDate, Task.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Task.PrioritySerNum
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum IN (8, 18, 19,22) AND Task.Status != 'Cancelled' AND Task.State != 'Deleted' AND Task.CreationDate > '2012-01-00 00:00:00' AND Task.PrioritySerNum=0 AND
    Task.DiagnosisSerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Task.DiagnosisSerNum, Priority.PriorityCode, Alias.AliasName, Task.CreationDate, Priority.CreationDate,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Priority.DueDateTime
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Priority ON Task.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum IN (8, 18, 19,22)  AND Task.Status != 'Cancelled' AND Task.State != 'Deleted' AND Task.DiagnosisSerNum=0  AND
    Task.CreationDate > '2012-01-00 00:00:00' AND (Priority.PriorityCode='SGAS_P3' OR Priority.PriorityCode='SGAS_P4') ''')
    taskdata=task_cur.fetchall()

    new_data=data+taskdata
    new_data.sort(key=lambda x:[x[0],x[4]])
    return new_data

def add_nopriorityEOTNT(DB,data):
    new_data = []
    EOTNT_cnx=mysql.connector.connect(user='root',password='service', database=DB)
    EOTNT_cur = EOTNT_cnx.cursor()

    EOTNT_cur.execute('''SELECT  Patient.PatientSerNum, Diagnosis.DiagnosisCode, Task.PrioritySerNum, Alias.AliasName, Task.DueDateTime, Task.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Task.PrioritySerNum
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Task.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum=6 AND Task.Status != 'Cancelled'  AND Task.State != 'Deleted' AND Task.CreationDate > '2012-01-00 00:00:00' AND Task.PrioritySerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Task.DiagnosisSerNum, Task.PrioritySerNum, Alias.AliasName, Task.DueDateTime, Task.PrioritySerNum,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Task.PrioritySerNum
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum = 6 AND Task.Status != 'Cancelled' AND Task.State != 'Deleted' AND Task.CreationDate > '2012-01-00 00:00:00' AND Task.PrioritySerNum=0 AND
    Task.DiagnosisSerNum=0
    UNION ALL
    SELECT  Patient.PatientSerNum, Task.DiagnosisSerNum, Priority.PriorityCode, Alias.AliasName, Task.DueDateTime, Priority.CreationDate,
    Patient.Sex, Patient.DateOfBirth,Task.ActivityInstanceAriaSer, Task.CompletionDate, Priority.DueDateTime
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Priority ON Task.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum = 6  AND Task.Status != 'Cancelled' AND Task.State != 'Deleted' AND Task.DiagnosisSerNum=0  AND
    Task.CreationDate > '2012-01-00 00:00:00' AND (Priority.PriorityCode='SGAS_P3' OR Priority.PriorityCode='SGAS_P4')''')
    EOTNTdata=EOTNT_cur.fetchall()

    new_data=data+EOTNTdata
    new_data.sort(key=lambda x:[x[0],x[4]])
    return new_data


def dosimetry(DB):
    d_cnx=mysql.connector.connect(user='root',password='service', database=DB)
    d_cur=d_cnx.cursor()
    ### Fetch data from database
    
    d_cur.execute('''SELECT Patient.PatientSerNum,Diagnosis.DiagnosisCode,Priority.PriorityCode,Alias.AliasName,Task.CreationDate,Priority.CreationDate,Patient.Sex,Patient.DateOfBirth,Task.ActivityInstanceAriaSer,Task.CompletionDate
    FROM Task JOIN Patient ON Task.PatientSerNum=Patient.PatientSerNum 
    JOIN Diagnosis ON Task.DiagnosisSerNum = Diagnosis.DiagnosisSerNum
    JOIN Priority ON Task.PrioritySerNum = Priority.PrioritySerNum 
    JOIN Alias ON Task.AliasSerNum = Alias.AliasSerNum 
    WHERE Task.AliasSerNum=22 AND Task.CreationDate > '2012-01-01 00:00:00' ''')

     
    data=d_cur.fetchall()
    new_data =[]
    for i in data:
        if i[2] in ['SGAS_P3','SGAS_P4']:
            new_data.append(list(i))
    return new_data

### filter out duplicate ONLY WHEN THEY ARE CONSECUTIVE keeping only the first instance
def filter_out_duplicates(data):
    print('-------------- filtering out duplicates --------------')
    start=time.time()
    filtered_data = []
    patientID=data[0][0]
    last_alias='NONE'
    last_priority='NONE'
    for i in data:
        current_alias = i[3]
        current_priority = i[2]
        if i[0] == patientID:
            if current_alias != last_alias:
                filtered_data.append(i)
                last_alias=current_alias
                last_priority=current_priority
            elif current_alias == last_alias and current_priority!=last_priority:
                del filtered_data[-1]
                filtered_data.append(i)
                last_alias=current_alias
                last_priority=current_priority
            else:
                continue
        else:
            filtered_data.append(i)
            last_alias = current_alias
            last_priority = current_priority
            patientID = i[0]
    end = time.time()
    print('---------------- Done in %.3f seconds ---------------\n' %(end-start))
    return filtered_data

### get a list of all occuring sequences and record all patients that never get to READY FOR TREATMENT
def filter_out_incompletes(filtered_data):
    print('--------- filtering out incomplete sequences ---------')
    start=time.time()
    patient = []
    incomplete_patient=[]                              
    sequenceDB =[]
    sequence = []
    patientID=filtered_data[0][0]
    patient.append(patientID)

    for i in filtered_data:
        if i[0] == patientID:
            sequence.append(i[3])
        else:
            sequenceDB.append(sequence.copy())
            if 'READY FOR TREATMENT' not in sequence:  
                incomplete_patient.append(patientID)   
            sequence.clear()
            patientID=i[0]
            patient.append(patientID)
            sequence.append(i[3])
    sequenceDB.append(sequence)
    if 'READY FOR TREATMENT' not in sequence:          
        incomplete_patient.append(patientID)           

    ### Filter Data once more to remove patients that never actually got to Ready For Treatment
    ### or 'First Treatment'
    filtered_data_2 = []
    for i in filtered_data:
        if i[0] in incomplete_patient:
            continue
        else:
            filtered_data_2.append(i)

    end=time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))
    return filtered_data_2


def get_MR_time(data):
    new_data = []
    for i in data:
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
        new_data.append((i[0],i[1],i[2],i[3],i[4],creation)+i[6:])
    return new_data

### Now insert 'Medically Ready' task into sequences

def insert_MR(data):
    print('----------- inserting Medically Ready Task -----------')
    start = time.time()
    new_data = []
    final_data = []
    # create a list to keep track of patients that follow illogical Ready For Treatment times
    bad_patients = []
    patientID = data[0][0]
    # set a flag to know whether 'Medically Ready' task has been established already for the patient
    flag = False
    if data[0][5] < data[0][4]:
        new_data.append((data[0][0],data[0][1],data[0][2],'MEDICALLY READY',data[0][5]) + data[0][5:])
        flag = True
    for i in data:
        if i[0] != patientID:
            flag = False
            patientID=i[0]
            if i[5] == None or i[5]=='0':
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
                continue
            if i[5]<i[4] and flag==False:
                new_data.append((i[0],i[1],i[2],'MEDICALLY READY',i[5]) + i[5:])
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
                flag = True
            else:
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
        else:
            if len(new_data)==0:
                pass
            else:
                if i[5] != new_data[len(new_data)-1][5]:
                    flag=False
            if i[5] == None or i[5]=='0':
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
                continue
            if i[5]<i[4] and flag==False:
                new_data.append((i[0],i[1],i[2],'MEDICALLY READY',i[5]) + i[5:])
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
                flag = True
            else:
                new_data.append((i[0],i[1],i[2],i[3],i[4]) + i[5:])
    

    ### make sure that data is ordered by date
    new_data.sort(key=lambda x: [x[0],x[4]])

    ### remove last column
    for i in new_data:
        if len(i)> 6:
            final_data.append(list(i[:5]+i[6:]))
        else:
            final_data.append(list(i[:5]))
    end = time.time() 
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))
    return final_data            


### analyze the sequences and find which patients are missing an MD contour,
### or a Dose calculation, hen delete these sequences


def delete_incomplete_sequences(data,alias_index):
    print('----------- deleting incomplete sequences ------------')
    start = time.time()
    MD   = []
    DOSE = []


    patientID = data[0][0]
    flag_MD   = False
    flag_DOSE = False


    for i in data:        
        if i[0] != patientID:            
            if flag_MD == False:
                MD.append(patientID)
            if flag_DOSE == False:
                DOSE.append(patientID)
                
            flag_MD   = False
            flag_DOSE = False

            if i[alias_index] == 'READY FOR MD CONTOUR':
                flag_MD = True
            elif i[alias_index] == 'READY FOR DOSE CALCULATION':
                flag_DOSE = True
            patientID = i[0]
            
        else:
                  
            if i[alias_index] == 'READY FOR MD CONTOUR':
                flag_MD = True            
            elif i[alias_index] == 'READY FOR DOSE CALCULATION':
                flag_DOSE = True                                
            else:
                continue

    # Filter out patients that do not have an MD or DOSE in their sequence... too unreliable

    filtered_data=[]
    count=0
    for i in data:
        if i[0] in MD or i[0] in DOSE:
            count+=1
            continue
        else:
            filtered_data.append(i)
            
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))
    return filtered_data


### Function that looks at each patient and cuts his sequence into subsequences if he goes through more than 1 treatment
def cut_sequences(data):
    print('------------- breaking sequences apart ---------------')
    start = time.time()
    new_data  = []
    patientID = data[0][0]
    n=0
    line=[]
    flag=False
    for i in data:
        if i[0] != patientID:
            flag=False
            patientID=i[0]
            n+=1
            new_data.append([n]+i)
            if i[3] == 'MEDICALLY READY':
                flag = True
        else:
            if i[3] == 'End of Treament Note Task':
                new_data.append([n]+i)
                flag = False
                n+=1
            elif i[3] == 'MEDICALLY READY' and flag==False:
                new_data.append([n]+i)
                flag = True
            elif i[3] == 'MEDICALLY READY' and flag==True:
                new_data.append([n]+i)
                line.append(n)
            else:
                new_data.append([n]+i)
                
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))            
    return new_data,line

### Some sub-sequences have 2 Medically Ready tasks, only keep the most recent
def del_duplicate_MR(data,duplicates):
    print('---- Keeping only last MEDICALLY READY instance ------')
    start = time.time()
    new_data = []
    for i in range(len(data)-1):
        if data[i][0] in duplicates and data[i][4] == 'MEDICALLY READY':
            if data[i+1][4] == 'MEDICALLY READY':
                continue
            else:
                new_data.append(data[i])
        else:
            new_data.append(data[i])

    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return new_data
                

### Function that looks at sub-sequences and keeps only first instance of alias
def first_instances(data):
    print('---------- retaining only first instances ------------')
    start = time.time()
    new_data = []
    sequence=[]
    patientID = data[0][0]
    for i in data:
        if i[0] != patientID:
            sequence.clear()
            patientID=i[0]
            sequence.append(i[4])
            new_data.append(i)
        else:
            if i[4] not in sequence:
                new_data.append(i)
                sequence.append(i[4])
            else:
                continue
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return new_data

### Final filter to get rid of the subsequences that do not get to Ready For Treatment or First Treatment
def final_filter(data):
    print('--------- filter out incomplete sequences ------------')
    start = time.time()
    new_data = []
    patientID=data[0][0]
    incomplete_patient=[]
    flag=False
    for i in data:
        if i[0]!=patientID:
            if flag == False:
                incomplete_patient.append(patientID)
            patientID = i[0]
            flag = False
            if i[4] == 'READY FOR TREATMENT' or 'First Treatment':
                flag = True
        else:
            if i[4] == 'READY FOR TREATMENT' or 'First Treatment':
                flag = True

    for i in data:
        if i[0] not in incomplete_patient:
            new_data.append(i)
        else:
            continue

    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return new_data


# This function check each sequence and makes sure that MEDICALLY READY,READY FOR MD CONTOUR,READY FOR DOSE CALCULATION
# and READY FOR TREATMENT Are in fact in the right order 
def right_sequence(data):
    print('------ Deleting sequences that are out of order ------')
    start = time.time()
    perfect_sequence = ['Ct-Sim','READY FOR MD CONTOUR','READY FOR DOSE CALCULATION', 'READY FOR TREATMENT']
    patientID = data[0][0]
    if data[0][4] in perfect_sequence:
        ind = perfect_sequence.index(data[0][4])
    else:
        ind=-1
    new_data = []
    bad_sequences = [] 

    # Find sequences that are out of order
    for i in data:
        if i[0] != patientID:
            ind=-1
            if i[4] in perfect_sequence:
                ind = perfect_sequence.index(i[4])
            patientID = i[0]
        else:
            if i[4] in perfect_sequence:            
                if perfect_sequence.index(i[4]) < ind:
                    bad_sequences.append(patientID)
                ind = perfect_sequence.index(i[4])

    # Delete out of order sequences
    for i in data:
        if i[0] in bad_sequences:
            continue
        else:
            new_data.append(i)
            
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return new_data

# Dates are not extremely reliable, and so if we inspect the data, we must
# set limits to the amount of time each step can take. If a sequence cannot satisfy
# these time constraints, it should be removed from the data
def delete_irregular_sequences(data):
    print('------------ delete irregular sequences --------------')
    start = time.time()    
    irregular_patients = []
    new_data = []
    patientID = data[0][0]

    # Loop will find patients that have a stage thats lasts irregularly long

    for i in range(len(data)-1):
        if data[i+1][0] != data[i][0] or data[i][4] in ['READY FOR TREATMENT','First Treatment']:
            continue
        else:
            if data[i][4] == 'MEDICALLY READY':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 8:
                    irregular_patients.append(data[i][0])

            elif data[i][4] == 'Ct-Sim':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 6:
                    irregular_patients.append(data[i][0])
            
            elif data[i][4] == 'READY FOR MD CONTOUR':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 8:
                    irregular_patients.append(data[i][0])

            elif data[i][4] == 'READY FOR DOSE CALCULATION':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 8:
                    irregular_patients.append(data[i][0])

            elif data[i][4] == 'PRESCRIPTION APPROVED':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 5:
                    irregular_patients.append(data[i][0])

            elif data[i][4] == 'Prescription Document (Fast Track)':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 5:
                    irregular_patients.append(data[i][0])

            elif data[i][4] == 'READY FOR PHYSICS QA':
                timediff = DayDifference(data[i][5],data[i+1][5],'QC')
                if timediff > 5:
                    irregular_patients.append(data[i][0])

            else:
                continue
    irregular_patients = set(irregular_patients)
    # Delete the irregular sequences from the data                
    for i in data:
        if i[0] not in irregular_patients:
            new_data.append(i)

    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return new_data        
            
def get_cancer_type(data,DB):
    # query database to get cancer types  associated with diagnosis code
    print('-------- Replace Cancer Code by Cancer Type ----------')
    start = time.time()
    diagnosiscode,cancer = get_cancer(DB)
    new_data = []
    for i in data:
        try:
            cantype = cancer[diagnosiscode.index(i[2])]
            new_data.append(i[:2] + [cantype] + i[3:])
        except ValueError:
            new_data.append(i[:2] + ['Other'] + i[3:])
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))
    file = open('filtered_data.pkl','wb')
    pickle.dump(new_data,file)
    file.close() 
    return new_data

# This function takes the filtered data and places it in a dictionary
def create_dictionary(data):
    print('------------ Creating Dictionary of Data -------------')
    start = time.time()
    DataDict = {}
    MR   = []
    CT   = []
    MD   = []
    DOSE = []
    PRES = []
    PHYS = []
    RFT  = []
    for i in data:
        if i[4] == 'MEDICALLY READY':
            MR.append(i)
        elif i[4] == 'Ct-Sim':
            CT.append(i)
        elif i[4] == 'READY FOR MD CONTOUR':
            MD.append(i)
        elif i[4] == 'READY FOR DOSE CALCULATION':
            DOSE.append(i)
        elif i[4] in ['Prescription Document (Fast Track)', 'PRESCRIPTION APPROVED']:
            PRES.append(i)
        elif i[4] == 'READY FOR PHYSICS QA':
            PHYS.append(i)
        elif i[4] == 'READY FOR TREATMENT':
            RFT.append(i)
    DataDict['MR'],DataDict['CT'],DataDict['MD'],DataDict['DOSE'],DataDict['PRES'],DataDict['PHYS'],DataDict['RFT'] = MR,CT,MD,DOSE,PRES,PHYS,RFT

    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))    
    return DataDict


def get_dosimetry_load(datadict,DB):
    print('-------------- Adding Dosimetry Loads ----------------')
    start = time.time()
    new_data = []
    dosdata = dosimetry(DB)
    data = datadict['DOSE']
    for i in data:
        load = 0
        for j in dosdata:
            if j[9] == None:
                continue
            if i[5]>=j[4] and i[5]<=j[9]:
                load+=1
        i.append(str(load))
        new_data.append(i)
    datadict['DOSE'] = new_data
    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start))   
    return datadict

def get_MR_load(datadict):
    print('------------------ Adding MD Loads -------------------')
    start = time.time()
    timetable=[]
    new_MR_data=[]
    for i in datadict['MR']:
        for j in datadict['RFT']:
            if i[0]==j[0]:
                timetable.append([i[5],j[5]])
            
    for i in datadict['MR']:
        load = 0
        for j in timetable:
            if i[5]>=j[0] and i[5]<=j[1]:
                load+=1
        i.append('MRLOAD: '+str(load))
        new_MR_data.append(i)

    datadict['MR'] = new_MR_data
    file = open('datadict.pkl','wb')
    pickle.dump(datadict,file)
    file.close()

    end = time.time()
    print('---------------- done in %.3f seconds ---------------\n' %(end-start)) 
    return datadict

DB='devAEHRA'
#def ExtractData(DB):
raw_data0 = query_database(DB)
print('rawdata0')
print(len(raw_data0))
raw_data00 = add_nopriorityCT(DB,raw_data0)

print('rawdata00')
print(len(raw_data00))
raw_data000 = add_nopriorityTasks(DB,raw_data00)
print('rawdata000')
print(len(raw_data000))
raw_data = add_nopriorityEOTNT(DB,raw_data000)
print('rawdata')
print(len(raw_data))

filtered_data1  = filter_out_duplicates(raw_data)
print('filtered_data1')
print(len(filtered_data1))
filtered_data2 = filter_out_incompletes(filtered_data1)
print('filtered_data2')
print(len(filtered_data2))
filtered_data2 = get_MR_time(filtered_data2)
print('filtered_data2')
print(len(filtered_data2))
filtered_data3 = insert_MR(filtered_data2)
print('filtered_data3')
print(len(filtered_data3))
filtered_data4 = delete_incomplete_sequences(filtered_data3,3)
print('filtered_data4')
print(len(filtered_data4))
filtered_data5,duplicates = cut_sequences(filtered_data4)
print('filtered_data5')
print(len(filtered_data5))
filtered_data6 = del_duplicate_MR(filtered_data5,duplicates)
print('filtered_data6')
print(len(filtered_data6))
filtered_data7 = delete_incomplete_sequences(filtered_data6,4)
print('filtered_data7')
print(len(filtered_data7))
filtered_data8 = first_instances(filtered_data7)
print('filtered_data8')
print(len(filtered_data8))
filtered_data9 = final_filter(filtered_data8)
print('filtered_data9')
print(len(filtered_data9))
filtered_data10 = right_sequence(filtered_data9)
print('filtered_data10')
print(len(filtered_data10))
filtered_data11 = delete_irregular_sequences(filtered_data10)
print('filtered_data11')
print(len(filtered_data11))
filtered_data12 = get_cancer_type(filtered_data11,DB)
print('filtered_data12')
print(len(filtered_data12))
filtered_data_dictionary = create_dictionary(filtered_data12)
data_dict = get_dosimetry_load(filtered_data_dictionary,DB)
data_dict2 = get_MR_load(data_dict)
    #return data_dict2


#26560



