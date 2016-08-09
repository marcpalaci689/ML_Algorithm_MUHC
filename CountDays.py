from datetime import datetime, timedelta as td
import holidays
import time
'''
This function takes 2 inputs: A creation date array and an End of Treatment
array and returns the number of days (fraction included) between the two excluding
weekends and holidays
'''

def DayDifference(start_date,end_date,Province):
    #start_time=time.time()
    timediff=[]
    for j in range(len(start_date)):
        if start_date[j] == '0':
            timediff.append('NA')
            continue
        d2=end_date[j]
        d1=start_date[j]
        # add fraction of day due to hour and minute difference
        count=(d2.hour-d1.hour)/24 + (d2.minute-d1.minute)/1440

        if d1.date() == d2.date():
            timediff.append(count)
        elif d1.date()>d2.date():
            delta= d1.date()-d2.date()
            for i in range(delta.days):
                day=d2+td(days=(i+1))
                if not (day.isoweekday() in [6,7]) and not (day in holidays.Canada(prov=Province)):
                    count-=1
            

            # input into timediff array
            timediff.append(count)
            
        else:
            delta= d2.date()-d1.date()
            for i in range(delta.days):
                day=d1+td(days=(i+1))
                if not (day.isoweekday() in [6,7]) and not (day in holidays.Canada(prov='QC')):
                    count+=1

            # input into timediff array
            timediff.append(count)
            
    #end_time = time.time()
    #print ('Time differences calculated in %f seconds' %(end_time - start_time))
    return timediff

