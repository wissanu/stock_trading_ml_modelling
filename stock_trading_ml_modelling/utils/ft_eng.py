#Import libraries
import pandas as pd
import numpy as np
import datetime as dt

####################
### EMA AND MACD ###
####################

#Function for calculating ema
def calc_ema(s_in, periods, lead_nan:int=0):
    """Function used to create EMA for a series
    
    args:
    -----
    s_in - pandas series - series of float values
    periods - int - value describing how far to look at for EMA calc
    lead_nan - int:0 - value describing the number of nans at the start which should be ignored

    returns:
    ------
    pandas series    
    """
    #Calc mod val
    mod = 2 / (periods+1)

    li_out = []
    thresh_i = s_in.index.min() + lead_nan + periods - 1
    for i, v in s_in.iteritems():
        if i < thresh_i:
            ema = None
        elif i == thresh_i:
            ema = np.nanmean(s_in.iloc[lead_nan:lead_nan + periods + 1])
        else:
            ema = (v * mod) + (prev_ema * (1 - mod))
        prev_ema = ema
        li_out.append(ema)
    s_out = pd.Series(li_out)
    s_out.index = s_in.index
    return s_out

    # #Weighting series
    # weight_ar = np.array(range(0, periods))
    # weight_ar = (1-mod)**weight_ar
    # #Calc cum emas
    # ema_s = [0] * s_in.shape[0]
    # for i in range(0, periods):
    #     ema_s += s_in.shift(i) * weight_ar[i]
    # #Calc ema
    # ema_s =  ema_s / np.sum(weight_ar)
    # return ema_s.copy()

#Function for calculating the MACD
def calc_macd(s_in, ema_lng:int=26, ema_sht:int=12, sig_period:int=9):
    """Function used to create MACD for a series
    
    args:
    -----
    s_in - pandas series - values to be converted to macd
    ema_lng - int:26 - the period over which the long ema will be calculated
    ema_sht - int:12 - the period over which the short ema will be calculated
    sig_period - int:9 - the period over which the macd will be smoothed as an ema
    
    returns:
    ------
    tuple of pandas series, pandas series, pandas series - MACD line, signal line, macd histogram  
    """
    
    tmp_df = s_in.to_frame()
    tmp_df['ema_lng'] = calc_ema(s_in, ema_lng)
    tmp_df['ema_sht'] = calc_ema(s_in, ema_sht)
    #Calc the signal line
    tmp_df['macd_line'] = tmp_df.ema_sht - tmp_df.ema_lng
    tmp_df['signal_line'] = calc_ema(tmp_df.macd_line, sig_period, lead_nan=ema_lng)
    tmp_df['macd_hist'] = tmp_df.macd_line - tmp_df.signal_line
    return (tmp_df.ema_sht, tmp_df.ema_lng, tmp_df.macd_line, tmp_df.signal_line, tmp_df.macd_hist) 

#Calc the ema and macds for the data
def calc_ema_macd(df_in, ema_lng:int=26, ema_sht:int=12, sig_period:int=9):
    """Function used to call EMA and MACD functions
    
    args:
    -----
    df_in - pandas dataframe - must have columns 'close' and 'date' 
    ema_lng - int:26 - the period over which the long ema will be calculated
    ema_sht - int:12 - the period over which the short ema will be calculated
    sig_period - int:9 - the period over which the macd will be smoothed as an ema

    returns:
    ------
    pandas dataframe - with new columns for ema12, ema26, macd_line, signal_line, macd
    """
    tick_df = df_in.copy()
    try:
        #Add in the ema and macd
        tick_df = tick_df.sort_values(by='date')
        tick_df['ema12'],tick_df['ema26'],tick_df['macd_line'],tick_df['signal_line'],tick_df['macd'] = calc_macd(tick_df['close'],ema_lng=ema_lng, ema_sht=ema_sht, sig_period=sig_period)
        return tick_df
    except Exception as e:
        print('ERROR:{}'.format(e))
        return tick_df



###################
### NORMALISING ###
###################

#Create a function which normalises a feature based only on the values which have come before it - avoids time series bias
def norm_time_s(ind:int, s_in, window:int, neg_vals:bool=False, mode:str='max_min',return_series:bool=False, fill_window:bool=False):
    """Normalise a value within over a time period
    
    args:
    -----
    ind - int - the index of this value in the series
    s_in - pandas series - a series of values to be normalised
    window - int - the number of values to look over
    neg_vals - bool:False - is the output to accunt for the sign of values
    mode  str:max_min - should the normalisation be done by max mins or standard deviation
    return_series - bool:Fasle - Should the return be a series or a value
    fill_window - bool:False - Should the returned series be bulked up to fit the window

    returns:
    ------
    float - normalised value in the window period
    """
    #Establish the index window
    this_ind = ind - s_in.index.min()
    if this_ind < window:
        st_ind = 0
    else:
        st_ind = this_ind - window + 1
    s = s_in[st_ind:this_ind+1]
    if return_series:
        v = s
    else:
        v = s_in[ind]
    #Normalise the value
    if mode == 'max_min':
        min = np.nanmin(s.values)
        max = np.nanmax(s.values)
        #If accounting for neg_vals then adjust max and min to allow this
        # This method allows values to be normalised and be relative to each other (IE -25 is half the magnitude of -50 and 50)
        if neg_vals:
            max = np.nanmax([np.abs(min),max])
            min = 0
        norm_val = (v - min) / (max - min)
    elif mode == 'std':
        if neg_vals:
            if v < 0:
                s = s[s <= 0]
            else:
                s = s[s >= 0]
            mean = np.nanmean(s.values)
            std = np.nanstd(s.values)
            norm_val = (v - mean) / std
        else:
            mean = np.nanmean(s.values)
            std = np.nanstd(s.values)
            norm_val = (v - mean) / std
    else:
        raise ValueError('mode must be "std" or "max_min", {} given'.format(mode))
    if return_series and fill_window:
            leading_s = pd.Series([np.nan] * (window - norm_val.shape[0]))
            norm_val = leading_s.append(norm_val)
    return norm_val

#Run the functions
def norm_prices(df_in, norm_window:int):
    """Function used to normalise all prices in the dataframe
    
    args:
    -----
    df_in - pandas dataframe - must contain values 'open','close','high','low','volume'
    norm_window - int - the period over which values will be normalised
    
    returns:
    ------
    pandas dataframe - with normalised values for 'open','close','high','low','volume'
    """
    df_out = df_in.copy()
    #Normalise the columns which need it
    norm_cols = [
        #Standard features
        "open"
        ,"close"
        ,"high"
        ,"low"
        ,"volume"
    ]
    #Reset the index
    df_out.sort_values(['date'],ascending=True, inplace=True)
    #Normalise
    for col in norm_cols:
        df_out["{}orig".format(col)] = df_out[col].copy() #Take a copy so as the values are changed this does not affect following calculations
        df_out[col] = [norm_time_s(x, df_out["{}orig".format(col)],norm_window) for x in df_out.index]
    return df_out

#Get in-row price change
def calc_changes(s_in, prev_s_in):
    """Function used to calculate the change between two values, absolute and percentage
    
    args:
    -----
    s_in - pandas series - the current value to be compared
    prev_s_in - pandas series - the base value to compare the values to
    
    returns:
    ------
    tuple - pandas series, pandas series - absolute change, percentage change
    """
    s_change = s_in - prev_s_in
    s_per_change = s_change / s_in
    return (s_change, s_per_change)



##########################
### BUY SELL FUNCTIONS ###
##########################

#Check if the target price is hit within the target_price_period
def min_gain_check(var_s, target_s, periods:int=12):
    """Function used to check if the value meets the min gain criteria
    
    args:
    -----
    var_s - pandas series - value to be compared
    target_s - pandas series - the target value to be hit
    periods - int - time period to check for gain over
    
    returns:
    ------
    pandas series - bools
    """
    check_s = [False] * var_s.shape[0]
    for i in range(1, periods+1):
        tmp_check_s = var_s.shift(-i) > target_s #True if price is >= limit
        check_s = check_s | tmp_check_s
    return check_s

def max_drop_check(var_s, target_s, periods:int=12):
    """Function used to check if the value meets the max drop criteria
    
    args:
    -----
    var_s - pandas series - value to be compared
    target_s - pandas series - the target value to be hit
    periods - int - time period to check for gain over
    
    returns:
    ------
    pandas series - bools
    """
    check_s = [False] * var_s.shape[0]
    for i in range(1, periods+1):
        tmp_check_s = var_s.shift(-i) < target_s #True if price is <= limit
        check_s = check_s | tmp_check_s
    return check_s

def close_vs_close(var_s, shift:int=1):
    """Function used to calculate the change over a given period
    
    args:
    -----
    var_s - pandas series - values to be compared
    shift - int - time period to shift var_s over for comparison
    
    returns:
    ------
    pandas series - floats
    """
    check_s = var_s.shift(shift) - var_s
    return check_s

#Create a function for finding buy signals
def get_buys(var_s, period, min_gain, max_drop):
    """Function used to find if a value meets the requirements for a buy signal
    
    args:
    -----
    var_s - pandas series - values to be checked
    period - int - window in which values must be reached or avoided
    min_gain - float - the minimum gain to be hit (%)
    max_drop - float - the maximum drop allowed (%)
    
    returns:
    ------
    pandas series - bools
    """
    
    #Check if the target price is hit within the period
    target_s = var_s * (1+min_gain)
    min_gain_s = min_gain_check(var_s, target_s, period) == True #Function returns True when min_gain is hit
    print('BUY min_gain_s -> {}'.format(min_gain_s[min_gain_s == True].shape))
    
    #Check if the sell price is hit within the period
    target_s = var_s * (1+max_drop)
    max_drop_s = max_drop_check(var_s, target_s, period) == False #Function returns False when does not go below target
    print('BUY max_drop_s -> {}'.format(max_drop_s[max_drop_s == True].shape))
    
    #Check if the following day is a positive change on today's close price
    close_vs_close_pos_s = close_vs_close(var_s, -1) > 0
    print('BUY close_vs_close_pos_s -> {}'.format(close_vs_close_pos_s[max_drop_s == True].shape))
    
    #Find the buy signals
    s_out = min_gain_s & max_drop_s & close_vs_close_pos_s
    print('BUY ALL -> {}'.format(s_out[s_out == True].shape))
    
    return s_out

#Function for finding sell signals
def get_sells(var_s, period, min_gain, max_drop):
    """Function used to find if a value meets the requirements for a sell signal
    
    args:
    -----
    var_s - pandas series - values to be checked
    period - int - window in which values must be reached or avoided
    min_gain - float - the minimum gain to be hit (%)
    max_drop - float - the maximum drop allowed (%)
    
    returns:
    ------
    pandas series - bools
    """
    
    #Check if the target price is hit within the period
    target_s = var_s * (1+max_drop)
    max_drop_s = max_drop_check(var_s, target_s, period) == True #Function returns True when max_drop is hit
    print('SELL max_drop_s -> {}'.format(max_drop_s[max_drop_s == True].shape))
    
    #Perform if the target is crossed again
    target_s = var_s * (1+min_gain)
    min_gain_s = min_gain_check(var_s, target_s, period) == False #Function returns False when min_gain is not hit
    print('SELL min_gain_s -> {}'.format(min_gain_s[min_gain_s == True].shape))
    
    #Check if the following day is a negative change on today's close price
    close_vs_close_neg_s = close_vs_close(var_s, -1) < 0
    print('SELL close_vs_close_pos_s -> {}'.format(close_vs_close_neg_s[max_drop_s == True].shape))
    
    #Find the sell signals
    s_out = max_drop_s & min_gain_s & close_vs_close_neg_s
    print('SELL ALL -> {}'.format(s_out[s_out == True].shape))
    
    return s_out



#####################
### MINS AND MAXS ###
#####################

#Mark minimums and maximums
def flag_mins(s_in, period:int=3, gap:int=3, cur:bool=False):
    """Function used to identify values in a series as mins
    
    args:
    -----
    s_in - pandas series - values to be compared
    period - int:3 - window to check values over
    gap - int:3 - the number of period which must have elapsed before a min is 
        identified (prevents changing of min_flags on current week vs same week next week)
    cur - bool:False - is this looking at current or past values
    
    returns:
    ------
    pandas series - bools
    """
    s_out = 0
    #Adjust the series input if looking at the current values (IE not able to see the future)
    if cur:
        s_in = s_in.shift(gap)
    #Looking back - check within window
    for i in range(1, period+1):
        s_out += (s_in > s_in.shift(i)) | (s_in.shift(i).isnull())
    
    #Looking forwards
    if cur:
        #Check within gap
        for i in range(1, gap+1):
            s_out += (s_in > s_in.shift(-i))
    else:
        #Check within forwardlooking periods
        for i in range(1, period+1):
            s_out += (s_in > s_in.shift(-i)) | (s_in.shift(-i).isnull())
    
    #Check end series
    s_out = s_out == 0
    
    return s_out

def flag_maxs(s_in, period:int=3, gap:int=0, cur:bool=False):
    """Function used to identify values in a series as maxs
    
    args:
    -----
    s_in - pandas series - values to be compared
    period - int:3 - window to check values over
    gap - int:3 - the number of period which must have elapsed before a min is 
        identified (prevents changing of min_flags on current week vs same week next week)
    cur - bool:False - is this looking at current or past values
    
    returns:
    ------
    pandas series - bools
    """
    s_out = 0
    #Adjust the series input if looking at the current values (IE not able to see the future)
    if cur:
        s_in = s_in.shift(gap)
    #Looking back - check within window
    for i in range(1, period+1):
        s_out += (s_in < s_in.shift(i)) | (s_in.shift(i).isnull())
    
    #Looking forwards
    if cur:
        #Check within gap
        for i in range(1, gap+1):
            s_out += (s_in < s_in.shift(-i))
    else:
        #Check within forwardlooking periods
        for i in range(1, period+1):
            s_out += (s_in < s_in.shift(-i)) | (s_in.shift(-i).isnull())
    s_out = s_out == 0
    return s_out

#Function to find last max and mins
def prev_max_min(df_in, var_col, bool_col, gap:int=0):
    """Function to find last max and mins
    
    args:
    -----
    df_in - pandas dataframe - must contain 'date', var_col and bool_col as column names
    var_col - str - the name of the column containing the current variables
    bool_col - str - the name of the column containing the bool values defining max and min vlaues
    gap - int:0 - the number of period which must have elapsed before a min is 
        identified (means that when you're in that week you can't tell if it's a min/max)
    
    returns:
    ------
    tuple - pandas series, pandas series, pandas series - last max/min value, last max/min date, last max/min index
    """
    df_in["prev_val"] = df_in.loc[df_in[bool_col].fillna(False),var_col]  
    df_in["prev_val"] = df_in["prev_val"].fillna(method='ffill').shift(gap)#Shift gap allows offset
    df_in["prev_marker_date"] = df_in.loc[df_in[bool_col].fillna(False),"date"]
    df_in["prev_marker_date"] = df_in["prev_marker_date"].fillna(method='ffill').shift(gap)#Shift gap allows offset
    df_in['index'] = df_in.index
    df_in["prev_marker_index"] = df_in.loc[df_in[bool_col].fillna(False),'index']
    df_in["prev_marker_index"] = df_in["prev_marker_index"].fillna(method='ffill').shift(gap)#Shift gap allows offset
    return (df_in["prev_val"],df_in["prev_marker_date"],df_in["prev_marker_index"])

#Function for finding the max within a given time period using indexes
def max_min_period(s_in, period:int=1, normalise:bool=False, max_min:str='max'):
    """Function for calculating the max and mins within a period
    
    args:
    -----
    s_in - pandas series - the vlaues to be looked at
    period - int:1 - the time window to look over
    max_min - str:max - looking for the max or min
    normalise - bool:False - should the returned value be normalised?
    
    returns:
    ------
    pandas series - floats
    """
    #Find the min index
    min_i = s_in.index.min()
    if normalise:
        s_max = pd.Series([s_in.loc[x-period if x-period >= min_i else min_i:x].max() for x in s_in.index])
        s_min = pd.Series([s_in.loc[x-period if x-period >= min_i else min_i:x].min() for x in s_in.index])
        s_out = pd.Series((s_in - s_min) / (s_max - s_min))
    else:
        #Get the max or min within a time period, ensuring not to go into negative indexes
        if max_min == 'max':
            s_out = pd.Series([s_in.loc[x-period if x-period >= min_i else min_i:x].max() for x in s_in.index])
        elif max_min == 'min':
            s_out = pd.Series([s_in.loc[x-period if x-period >= min_i else min_i:x].min() for x in s_in.index])
        else:
            raise ValueError('max_min must be either \'max\' or \'min\'')
    s_out.index = s_in.index
    return s_out



#################
### GRADIENTS ###
#################

#Function for finding the gradient of a variable overa set period
def gradient(s_in, period:int=1):
    """Function for finding the gradient of a variable over a set period
    
    args:
    -----
    s_in - pandas series - the series from which the gradient will be found
    period - int:1 - the period over which the gradient will be found
    
    returns:
    ------
    pandas series
    """
    s_out = s_in - s_in.shift(period)
    return s_out



###########################
### PROPORTIONAL VALUES ###
###########################

#Calc vol as proportion of previous n-rows
def calc_prop_of_prev(s_in, periods:int = 4):
    """Function to this value as a proportion of the cum previous values
    
    args:
    -----
    s_in - pandas series - values to be looked at
    period - int - window to sum values over
    
    returns:
    ------
    pandas series - floats
    """
    s_cum = s_in.copy()
    for i in range(1, periods):
        s_cum += s_in.shift(i)
    return s_in / s_cum

#Function for calculating the percentage change within a range
def per_change_in_range(s_in, period:int=1, **kwargs):
    """Function for calculating the percentage change of a value from it's max or min within a range
    
    args:
    -----
    s_in - pandas series - the values to be looked at
    period - int:1 - the time window to look over
    
    returns:
    ------
    pandas series - floats
    """
    return ((s_in - max_min_period(s_in, period, normalise=False, **kwargs)) / max_min_period(s_in, period, normalise=False, **kwargs))

def avg_in_range(s_in, period:int=1, inc_val:bool=True):
    """Function for calculating average within a range
    
    args:
    -----
    s_in - pandas series - the values to be looked at
    period - int:1 - the time window to look over
    inc_val - bool:True - should the average include the subject value
    
    returns:
    ------
    pandas series - floats
    """
    if inc_val:
        s_out = [s_in.iloc[x-period+1:x+1].mean() if x-period+1 > 0 else s_in.iloc[:x+1].mean() for x in range(s_in.shape[0])]
    else:
        s_out = [s_in.iloc[x-period:x].mean() if x-period > 0 else s_in.iloc[:x].mean() for x in range(s_in.shape[0])]

    return s_out



#########################
### POSITIVE NEGATIVE ###
#########################

#Mark points of macd positive entry
def pos_entry(s_in):
    """Function to check if this value is a new positive after a negative value
    
    args:
    -----
    s_in - pandas series - values to be looked at
    
    returns:
    ------
    pandas series - bools
    """
    return (s_in > s_in.shift(1)) & (s_in > 0) & (s_in.shift(1) < 0)

def neg_entry(s_in):
    """Function to check if this value is a new negative after a positive value
    
    args:
    -----
    s_in - pandas series - values to be looked at
    
    returns:
    ------
    pandas series - bools
    """
    return (s_in < s_in.shift(1)) & (s_in < 0) & (s_in.shift(1) > 0)

#Create separate columns for pos and neg values - allows for normalisation
def pos_neg_cols(s_in, gt_lt = "GT"):
    """Function to separate columns for pos and neg values - allows for normalisation
    
    args:
    -----
    s_in - pandas series - the vlaues ot be looked at
    gt_lt - str:'GT' - defines if looking for positive or negative values
    
    returns:
    ------
    tuple - pandas series, pandas series - bools, floats
    """
    if gt_lt.upper() == "GT":
        bool_s = s_in >= 0
    elif gt_lt.upper() == "LT":
        bool_s = s_in <= 0
    df_out = s_in.to_frame()
    df_out["s_in"] = s_in
    df_out["val"] = abs(s_in[bool_s])
    val_s = df_out["val"].fillna(0, method=None)
    return (bool_s, val_s)



#######################
### PREVIOUS VALUES ###
#######################

def mk_prev_move_float(s_in):
    """Function to find the the magnitude of the most recent value change.
    
    args:
    ------
    s_in - pandas series - float values
    
    returns:
    ------
    pandas series - float values
    """
    s_out = s_in - s_in.shift(1)
    s_out[s_out == 0] = np.nan
    s_out = s_out.fillna(method='ffill')
    return s_out

def mk_prev_move_date(s_in, periods:int=7):
    """Function to find the time elapsed between two different changes.
    
    args:
    ------
    s_in - pandas dataframe - datetime values
    periods - int:7 - used to modify days of datetime into the period required
    
    
    returns:
    ------
    pandas series - int values
    """
    s_out = s_in - s_in.shift(1)
    s_check = pd.Series([np.floor(x.days) for x in s_out])
    s_check[s_check == 0] = np.nan
    s_check = s_check.fillna(method='ffill')
    s_check = [np.floor(x/periods) for x in s_check]
    return s_check

#Create features for the cumulative sequential count of max/mins in a certain direction
def mk_move_cum(s_in):
    """Function for counting the number of changes of the same sign sequentially.
    EG how many positive moves have there been in a row.
    
    args:
    ------
    s_in - pandas series - floats
    
    returns:
    pandas series - floats
    """
    li_out = []
    prev_x = None
    #Loop through each value in s_in
    for i, x in s_in.iteritems():
        if np.isnan(x) or prev_x == None: #If this is the first value add it to the list
            li_out.append(0)
        else:
            prev_x = prev_x if not np.isnan(prev_x) else 0
            if ((x < 0) & (prev_x > 0)) or ((x > 0) & (prev_x < 0)): #If a sign change then reset to 0
                li_out.append(0)
            else:
                if prev_x != x: #if there has been a change in value from this and the previous value increment it by 1
                    if x > 0: #for positive value increment by 1
                        li_out.append(li_out[-1] + 1)                                    
                    else: #for negative values increment by -1
                        li_out.append(li_out[-1] - 1)
                else: #Otherwise just use the last added value
                    li_out.append(li_out[-1])
        prev_x = x
    return li_out

#Create features showing the value change since the first min/max
def mk_long_prev_move_float(ref_s, val_s):
    """Function to find the value change since the first max/min move in the current sequential series.
    
    args:
    ------
    ref_s - pandas series - the reference series from which changes will be detected
    val_s - pandas series - the values series from which outputs will be created
    
    returns:
    ------
    pandas series - float values
    """
    li_out = []
    st_x = None
    prev_x = None
    #Loop through each value in s_in
    for i, x in ref_s.iteritems():
        if np.isnan(x) or prev_x == None: #If this is the first value add it to the list
            li_out.append(0)
        else:
            prev_x = prev_x if not np.isnan(prev_x) else 0
            if ((x < 0) & (prev_x > 0)) or ((x > 0) & (prev_x < 0)): #If a sign change then reset to 0
                li_out.append(0)
                st_x = None
            else:
                if st_x == None: #If st_x has not been set yet set it to this value
                    st_x = val_s[i]
                li_out.append(val_s[i] - st_x) #Now calculate the difference and add it to the list
        prev_x = x
    return li_out

def mk_long_prev_move_date(ref_s, val_s, periods:int=7):
    """Function to find the date change since the first max/min move in the current sequential series.
    
    args:
    ------
    ref_s - pandas series - the reference series from which changes will be detected
    val_s - pandas series - the values series from which outputs will be created
    periods - int:7 - used to modify days of datetime into the period required
    
    returns:
    ------
    pandas series - int values
    """
    li_out = []
    st_x = None
    prev_x = None
    #Loop through each value in s_in
    for i, x in ref_s.iteritems():
        if np.isnan(x) or prev_x == None: #If this is the first value add it to the list
            li_out.append(0)
        else:
            prev_x = prev_x if not np.isnan(prev_x) else 0
            if ((x < 0) & (prev_x > 0)) or ((x > 0) & (prev_x < 0)): #If a sign change then reset to 0
                li_out.append(0)
                st_x = None
            else:
                if st_x == None: #If st_x has not been set yet set it to this value
                    st_x = val_s[i]
                li_out.append(np.floor((val_s[i] - st_x).days/periods)) #Now calculate the difference and add it to the list
        prev_x = x
    return li_out



######################
### COLUMN LENGTHS ###
######################

#Create a dictionary of max character lengths of fields for use later in h5 file appending
def get_col_len_s(s_in):
    """Get the max length of value in the series
    
    args:
    -----
    s_in - pandas series - series holding values to look at for max field lengths
    
    returns:
    ------
    float
    """
    tmp_s = pd.Series([len(str(x)) for x in s_in])
    return tmp_s.max()
    
def get_col_len_df(df_in):
    """Create a dictionary of max character lengths of fields for use later in h5 file appending
    
    args:
    -----
    df_in - pandas dataframe - dataframe holding values to look at for max field lengths
    
    returns:
    ------
    dictionary
    """
    col_lens = {}
    for c in df_in:
        col_lens[c] = get_col_len_s(df_in[c])
    return col_lens


