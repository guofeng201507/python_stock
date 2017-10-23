# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 23:18:53 2017

@author: guof
"""

import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn import svm

digits = datasets.load_digits()

#print(digits.data)
#print(digits.target)



clf = svm.SVC(gamma=0.001, C=100)

X,y = digits.data[:-10], digits.target[:-10]

clf.fit(X,y)

print(clf.predict(digits.data[:-5]))

#plt.imshow(digits.images[-5], cmap=plt.cm.gray_r, interpolation='nearest')

plt.imshow(digits.images[-3], cmap=plt.cm.gray_r, interpolation='nearest')

plt.show()