import BuildMatrices
from sklearn import cross_validation
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.cross_validation import train_test_split
import numpy as np
import pickle
import warnings
import copy

### This script is responsible for calling the data extraction script and matrix builder script in order to create the models for each stage.
### The models are then pickled so that they can easily be accessed later on
                

#def __main__():
dat,col = BuildMatrices.build_matrices('devAEHRA')

for key in dat:
    if key == 'RFT':
        continue
    print(key)
    X = dat[key][:,0:-1]
    y = dat[key][:,-1]
    print(X.shape)
   

    model  = GradientBoostingRegressor(loss = 'lad',n_estimators=100).fit(X,y)
    # save each model in a file named by the key
    file = open(key+'.pkl','wb')
    pickle.dump(model,file)
    file.close()
    # save the column dictionary (dictionary that has all the features for each key)
    file = open('columndictionary.pkl','wb')
    pickle.dump(col,file)
    file.close()
    
    print('Model for %s stage complete' %(key))
    scores = cross_validation.cross_val_score(model, X, y, cv=10, scoring = 'mean_absolute_error')
    mean_abs_err = np.abs(np.mean(scores))
    print('Done Gradient Boosting Regressor. Mean absolute error: %.3f \n' %(mean_abs_err))
    


