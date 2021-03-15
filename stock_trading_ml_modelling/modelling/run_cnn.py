"""Script to create the training data

GOAL
----
To create a classifier which will identify mins and max points in price charts
"""

import gc
import numpy as np
import pandas as pd
from pathlib import Path
from tensorflow.keras.metrics import SparseCategoricalAccuracy
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import load_model

from stock_trading_ml_modelling.libs.logs import log
from stock_trading_ml_modelling.libs.data import Data

from stock_trading_ml_modelling.modelling.training_data import TrainingData
from stock_trading_ml_modelling.modelling.resnet_model import FunnyResNet

class TrainModel:
    def __init__(self):
        pass

############################
### CREATE TRAINING DATA ###
############################
training_data = TrainingData(
    limit_id=None,
    folder="cnn"
)
training_data.create_data(weeks=70)
training_data.save_data()
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
#Compile the model
model = FunnyResNet(3)
model.compile(
    optimizer=Adam(learning_rate=1e-5),
    loss="sparse_categorical_crossentropy",
    metrics=[SparseCategoricalAccuracy(name="sca")]
)
#Add early stopping
early_stopping = EarlyStopping()
#Create the loss weightings
class_weight = {
    v:1 - (training_data.y_train == v).sum() / training_data.y_train.shape[0]
    for k,v in training_data.labels.items()
    }
#Fit the model
model.fit(
    training_data.X_train,
    training_data.y_train,
    epochs=50,
    validation_split=0.2,
    batch_size=32,
    callbacks=[early_stopping],
    class_weight=class_weight
    )

###EXPORT
path = Path("data", "models", "bsh_resnet")
model.save(path)

#########################################
### RUN VALIDATION DATA THROUGH MODEL ###
#########################################
"""Run the validation set through the model and then run the output from that 
through a simulation to find profit/loss of the model"""
#Free up memory
training_data.prices = None
training_data.X_train = None
training_data.y_train = None
training_data.X = None
training_data.y = None
gc.collect()
#Load
model = load_model(path)
#Evaluate the model
val_loss, val_acc = model.evaluate(training_data.X_test, training_data.y_test)

preds = model.predict(training_data.X_test)
preds = np.argmax(preds, axis=1)
#PPV
for k,v in training_data.labels.items():
    mask = preds == v
    this_preds = preds[mask]
    act = training_data.y_test[mask]
    tp = (act == this_preds).sum()
    fp = (act != this_preds).sum()
    ppv = tp / (tp + fp)
    print(f"ppv of {k} - {ppv:.4f} - tp {tp} - fp {fp} - tp + fn {(training_data.y_test == v).sum()}")
