"""File for self.data object. Computes and manipulates self.data"""
import pandas as pd
import numpy as np

from stock_trading_ml_modelling.utils.ft_eng import calc_ema, calc_macd, calc_ema_macd

class Data:
    """An object for an individual series set of data
    
    data will be stored in self.data
    """
    def __init__(self, data, window:int=52):
        #Convert the self.data to a pandas series (if not already)
        if not isinstance(data, pd.core.series.Series):
            data = pd.Series(data)
        self.data = data
        self.lead_nan = 0
        self.window = window

    def __call__(self):
        """Return the self.data attribute when called directly"""
        return self.data

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

    def norm_data_to_last(self):
        """Normalises data to last item in range"""
        return self.data / self.data.iloc[-1]

    def norm_data_max_value(self):
        """Normalises data to max item in range"""
        return self.data / self.data.max()

    def norm_data_max_min_value(self):
        """Normalises data to between min and max item in range"""
        return (self.data - self.data.min()) / (self.data.max() - self.data.min())

    def calc_ema(self, period, lead_nan:int=0):
        """Function to create an ema series from the data"""
        ema_data = calc_ema(self.data, period, self.lead_nan)
        return ema_data

    def calc_macd(self, ema_sht:int, ema_lng:int, sig_period:int):
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
        data = self.data
        #Adjust data shape to fill window
        if data.shape[0] < window:
            extra = window - data.shape[0]
            a = pd.Series([np.nan] * extra)
            data = pd.concat((data,a))
        #Calc the number of windows
        en_i = data.shape[0] - window + 1
        #Iterate over self.data and create output
        out = np.empty((en_i, window))
        index = []
        for i,j in enumerate(range(en_i)):
            out[i] = np.array([data.iloc[j:j+window]])
            index.append(data.index[j+window-1])
        return index, out

    #Mark minimums and maximums
    def flag_mins(self, period:int=3, gap:int=3, cur:bool=False):
        """Function used to identify values in a series as mins
        
        args:
        -----
        period - int:3 - window to check values over
        gap - int:3 - the number of period which must have elapsed before a min is 
            identified (prevents changing of min_flags on current week vs same week next week)
        cur - bool:False - is this looking at current or past values
        
        returns:
        ------
        pandas series - bools
        """
        s_out = pd.Series([0] * self.data.shape[0], index=self.data.index)
        #Adjust the series input if looking at the current values (IE not able to see the future)
        if cur:
            s_in = self.data.shift(gap)
        else:
            s_in = self.data
        #Looking back - check within window
        for i in range(1, period+1):
            s_out += (s_in > s_in.shift(i)) | (s_in.shift(i).isnull())
        #Looking forwards
        if cur:
            #Check within gap
            for i in range(1, gap+1):
                s_out += (s_in > s_in.shift(-i))
        else:
            #Check within forward looking periods
            for i in range(1, period+1):
                s_out += (s_in > s_in.shift(-i)) | (s_in.shift(-i).isnull())
        #Check end series
        s_out = s_out == 0
        #Cover nas
        s_out.loc[s_out.isnull()] = 0
        return s_out

    def flag_maxs(self, period:int=3, gap:int=0, cur:bool=False):
        """Function used to identify values in a series as maxs
        
        args:
        -----
        period - int:3 - window to check values over
        gap - int:3 - the number of period which must have elapsed before a min is 
            identified (prevents changing of min_flags on current week vs same week next week)
        cur - bool:False - is this looking at current or past values
        
        returns:
        ------
        pandas series - bools
        """
        s_out = pd.Series([0] * self.data.shape[0], index=self.data.index)
        #Adjust the series input if looking at the current values (IE not able to see the future)
        if cur:
            s_in = self.data.shift(gap)
        else:
            s_in = self.data
        #Looking back - check within window
        for i in range(1, period+1):
            s_out += (s_in < s_in.shift(i)) | (s_in.shift(i).isnull())
        #Looking forwards
        if cur:
            #Check within gap
            for i in range(1, gap+1):
                s_out += (s_in < s_in.shift(-i))
        else:
            #Check within forward looking periods
            for i in range(1, period+1):
                s_out += (s_in < s_in.shift(-i)) | (s_in.shift(-i).isnull())
        s_out = s_out == 0
        #Cover nas
        s_out.loc[s_out.isnull()] = 0
        return s_out


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