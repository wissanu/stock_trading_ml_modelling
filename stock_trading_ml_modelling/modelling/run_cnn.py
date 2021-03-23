"""Script to create the training data

GOAL
----
To create a classifier which will identify mins and max points in price charts
"""
from numpy import argmax

from stock_trading_ml_modelling.libs.logs import log

from stock_trading_ml_modelling.modelling.training_data import TrainingData
from stock_trading_ml_modelling.modelling.classifier_model import ClassifierModel


############################
### CREATE TRAINING DATA ###
############################
training_data = TrainingData(
    limit_id=None,
    folder="cnn"
)
#Create new data
training_data.create_data(weeks=52*10, force=True)
training_data.save_data()
#Use existing data
training_data.load_data()

###################
### TRAIN MODEL ###
###################
"""Create a clalable which a model can be input as a handler and predictions 
will be output.

Data will be split between training and validation through random sampling.

All model must have:
- Split data
- Train
- Save
- Run
methods
"""
cls_model = ClassifierModel(3, folder="cnn")
cls_model.train(training_data.X_train, training_data.y_train, training_data.labels)
cls_model.save_model()
cls_model.eval_model(training_data.X_test, training_data.y_test, training_data.labels)

#########################################
### RUN VALIDATION DATA THROUGH MODEL ###
#########################################
"""Run the validation set through the model and then run the output from that 
through a simulation to find profit/loss of the model"""
#Load
cls_model.load_model()
#Evaluate the model
val_loss, val_acc = cls_model.evaluate(
    training_data.X_test,
    training_data.y_test
    )

preds = cls_model.predict(training_data.X_test)
preds = argmax(preds, axis=1)
#PPV
for k,v in training_data.labels.items():
    mask = preds == v
    this_preds = preds[mask]
    act = training_data.y_test[mask]
    tp = (act == this_preds).sum()
    fp = (act != this_preds).sum()
    ppv = tp / (tp + fp)
    print(f"ppv of {k} - {ppv:.4f} - tp {tp} - fp {fp} - tp + fn {(training_data.y_test == v).sum()}")
