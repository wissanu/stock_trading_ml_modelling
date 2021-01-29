"""File for self.data object. Computes and manipulates self.data"""
import pandas as pd
import numpy as np

from stock_trading_ml_modelling.utils.ft_eng import calc_ema, calc_macd, calc_ema_macd

class Data:
    """An object for an individual series set of data
    
    data will be stored in self.data
    """
    def __init__(self, data):
        #Convert the self.data to a pandas series (if not already)
        if type(data) != pd.core.series.Series:
            data = pd.Series(data)
        self.data = data
        self.lead_nan = 0

    def calc_consec_gain(self):
        """Function to mark the consecutive gains in a seres of data
        
        returns:
        ----
        pandas series
        """
        #Mark where there is a gain
        gains = self.data > 0
        #Count the records between the current record and the last non-gain record
        cons_gains = [0]
        _ = [cons_gains.append(
            cons_gains[-1] + v
        ) if v else cons_gains.append(0) for v in gains]
        return pd.Series(cons_gains[1:], name="cons_gains")

    def calc_consec_loss(self):
        """Function to mark the consecutive losses in a seres of data
        
        returns:
        ----
        pandas series
        """
        #Mark where there is a loss
        losses = self.data < 0
        #Count the records between the current record and the last non-loss record
        cons_losses = [0]
        _ = [cons_losses.append(
            cons_losses[-1] + v
        ) if v else cons_losses.append(0) for v in losses]
        return pd.Series(cons_losses[1:], name="cons_losses")

    def norm_data(self, norm_data):
        """Normalises one dataset next to another
        DATASETS MUST BE OF IDENTICAL SHAPE"""
        return self.data / norm_data

    def norm_data_to_last(self, norm_data):
        """Normalises data to last item in range"""
        return self.data / norm_data

    def calc_ema(self, period, lead_nan:int=0):
        """Function to create an ema series from the data"""
        ema_data = calc_ema(self.data, period, self.lead_nan)
        return ema_data

    def calc_macd(self, ema_sht:int=None, ema_lng:int=None, sig_period:int=None):
        """Function to create a macd series from the data"""
        macd_data = calc_macd(self.data, ema_lng, ema_sht, sig_period)
        return macd_data

    def calc_grad(self):
        """Function to create a gradient series from the data"""
        grad = (self.data - self.data.shift(1)) / abs(self.data)
        return grad

    def build_moving_window_data(self, window=None, fillna=None):
        """Converts a dataset into a multi-layered numpy array of values, one value for
        each movement of the moving window"""
        if window is None:
            window = self.window
        #Calc the number of windows
        en_i = self.data.shape[0] - window + 1
        #Iterate over self.data and create output
        out = np.empty((en_i, window))
        for i,j in enumerate(range(en_i)):
            out[i] = np.array([self.data.iloc[j:j+window]])
        return out


class DataSet:
    """An object for a set of data"""
    def __init__(self):
        """self.data will be held in a datasets dictionary"""
        #None vars
        self.datasets = {}

    def add_dataset(self, data, label):
        data_obj = Data(data)
        setattr(self, label, data_obj)
        self.datasets[label] = data_obj