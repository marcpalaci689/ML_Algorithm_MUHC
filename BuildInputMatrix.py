import numpy as np

# This function will look at the trained sparse matrix column arrangement
# and given a new patient details will return the corresponding sparse matrix
# should input a list of patient details [age,alias,cancer,priority,doctor,dosimetrist,sex]
# and a column array that defines the sparse matrix arrangement

def CreateInputMatrix(patient,col):
    columns=col

    X = np.array([patient[0]])
    for i in columns:
        if i in patient[1:]:
            X=np.append(X,1)
        else:
            X=np.append(X,0)

    return X
