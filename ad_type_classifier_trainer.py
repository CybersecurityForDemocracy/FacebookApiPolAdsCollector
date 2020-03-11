#%%!/usr/bin/env python3

"""
Flower classification on the Iris dataset using a Naive Bayes
classifier and TensorFlow.
For more info: http://nicolovaligi.com/naive-bayes-tensorflow.html
"""

import random
from time import time

import numpy as np
import pandas
from IPython import embed
from matplotlib import colors
from matplotlib import pyplot as plt
from sklearn import metrics, preprocessing
from sklearn.externals import joblib
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import (BernoulliNB, ComplementNB, GaussianNB,
                                 MultinomialNB)
from sklearn.utils.extmath import density

#%% Loading data and removing uncategorized

df = pandas.read_csv('/home/divam/projects/ad_types_analysis/training_data/typed_ads_of_interest.csv')


#%%
df['ad_type']= df['ad_type'].str.upper()
df = df[df['ad_type']!='UNKNOWN']
# df = df[df['ad_type']!='INFORM']

#%% Splitting data into test, train and validate, vectorizing
state = random.randint(0,65535)
X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(df['processed_body'], df['ad_type'], random_state=state, test_size=.20)   
X_test_r, X_validate_r, y_test_r, y_validate_r = train_test_split(X_test_r, y_test_r, random_state=state, test_size=.50)   

#%% Selecting interesting features and transforming accordingly
data_prep_pipeline = Pipeline([
    ('tfid_vectorizer',TfidfVectorizer(sublinear_tf=True, stop_words='english')),
    ('select_best', SelectKBest(chi2, k=3000))])

X_train = data_prep_pipeline.fit_transform(X_train_r, y_train_r)
joblib.dump(data_prep_pipeline, 'data_prep_pipeline.pk1')

X_test = data_prep_pipeline.transform(X_test_r)
X_validate = data_prep_pipeline.transform(X_validate_r)

le = preprocessing.LabelEncoder()
y_train = le.fit_transform(y_train_r)
joblib.dump(le, 'ad_type_label_encoder.pk1')

y_test = le.transform(y_test_r)
y_validate = le.transform(y_validate_r)
#%% Setting up training
def benchmark(clf):
    print('Random seed for data split = ', state)
    print('_' * 80)
    print("Training: ")
    print(clf)
    t0 = time()
    clf.fit(X_train, y_train)
    train_time = time() - t0
    print("train time: %0.3fs" % train_time)

    t0 = time()
    pred = clf.predict(X_test)
    test_time = time() - t0
    print("test time:  %0.3fs" % test_time)

    score = metrics.accuracy_score(y_test, pred)
    print("accuracy:   %0.3f" % score)

    if hasattr(clf, 'coef_'):
        print("dimensionality: %d" % clf.coef_.shape[1])
        print("density: %f" % density(clf.coef_))
        print()


    print("confusion matrix:")
    print(le.get_params())
    print(metrics.confusion_matrix(le.inverse_transform(y_test), le.inverse_transform(pred), labels=le.classes_))

    print("classification report:")
    print(metrics.classification_report(le.inverse_transform(y_test), le.inverse_transform(pred), target_names=le.classes_))
    metrics.plot_confusion_matrix(clf, X_test, y_test, display_labels=le.classes_, normalize='true')
    metrics.plot_confusion_matrix(clf, X_test, y_test, display_labels=le.classes_, normalize=None)

    clf_descr = str(clf).split('(')[0]
    return clf_descr, score, train_time, test_time
#%% Training and benchmarking against test set

clfr = ComplementNB(alpha=0.03)
print(benchmark(clfr))
joblib.dump(clfr,'ad_type_classifier.pk1')
# print(benchmark(GaussianNB()))
# print(benchmark(MultinomialNB()))

# print(benchmark(BernoulliNB()))

#%% Validating against validation set
