''' 
This script is only written so that the Prediction_Algorithm can easily be called as an absolute command through the command line
'''


import Prediction_Algorithm as PA
import sys
if len(sys.argv) == 2:
	PA.Predict(sys.argv[1])
else:
	history={'response':'Failed','Reason':'argument invalid'}
	print(history)
