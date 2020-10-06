The desired approach for predicting which stocks to buy and which not to buy is going to be done using a deep CNN which will take in the below features, each as a time series and process each one with 3 different sizes of network being layered over each other (similar to a YOLOv3 model).

Once all the features have been output they will be layered over each other to produce the final prediction.

The features to produce a time series for are:
- close price - relative to and normalise to latest value
- MACD-short term - normalised to latest close price
- MACD-long term - normalised to latest close price
- low-high price change in day