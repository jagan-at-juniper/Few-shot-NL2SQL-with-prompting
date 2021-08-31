import pandas as pd
import numpy as np
import re
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import warnings
from statsmodels.tsa.api import ExponentialSmoothing
warnings.filterwarnings("ignore")


# UNIVARIATE FEATURE SELECTION & ANOMALY DETECTION

def remove_known_outliers(df, date_col, start_remove, end_remove):
    """
    Takes in dataframe and starting and ending epoch
    (in ms) of known outliers. Returns dataframe without
    set of continuous outliers.

    Parameters:
        df (pandas dataframe): dataframe we want to analyze
        date_col (str): name of date/time column
        start_remove (int): starting epoch of known outliers in milliseconds
        end_remove (int): ending epoch of known outliers in milliseconds

    Returns:
        df (pandas dataframe): dataframe without a known outliers (continuous)
    """
    df = df.sort_values(by=[date_col])
    start_remove = pd.to_datetime(start_remove,unit='ms')
    end_remove = pd.to_datetime(end_remove,unit='ms')
    df[date_col] = pd.to_datetime(df[date_col])
    df.set_index(date_col, inplace=True)
    return df.loc[(df.index < start_remove) | (df.index > end_remove)]

# STATSMODELS EXPONENTIALSMOOTHING

def fit_sm_ExponentialSmoothing(df, date_col, metric_col, resample, resamp_freq=None):
    """
    Takes in dataframe,
    metric (string), resample frequency (string).
    Fits and returns two dataframes. One is
    the original dataframne after resampling.
    The other is a dataframe of the data's
    decomposition.

    Parameters:
        df (pandas dataframe): dataframe we want to analyze
        date_col (str): name of date/time column
        metric_col (str): name of column we want to analyze
        resample (bool): True if resampling needed, False to keep current timestamp
        resamp_freq (str): resample frequency if necessary

        NOTE: resamp_freq must be higher than original frequency

    Returns:
        df_resample (pandas dataframe): dataframe after resampling - later used to forecast & detect anomalies
        states (pandas dataftame): dataframe with time series decomposition (level, seasonality, residual)
    """
    df = df[[date_col, metric_col]]
    df[date_col] = pd.to_datetime(df.date)
    df = df.sort_values([date_col])
    df = df.set_index(date_col)
    df.columns = ['value']
    # if we want to resample
    if resample:
        df_resample = df.resample(resamp_freq).mean()
        if 'T' in str(resamp_freq):
            period = 60 / (int(re.findall("\d+", resamp_freq)[0]))
        elif 'S' in str(resamp_freq):
            period = (60 / (int(re.findall("\d+", resamp_freq)[0]))) * 60
    else:
        df_resample = df
        period = (int(re.findall("\d+", pd.infer_freq(df_resample.index))[0]))
    # period = number of data points per hour after resampling
    # seasonal_periods = (24 hours in a day)*period+1(make odd number as needed from statsmodels)
    mod = ExponentialSmoothing(df_resample, seasonal_periods=24 * period + 1,
                               trend='add', seasonal='add',
                               use_boxcox=None, initialization_method="estimated",
                               missing='drop')

    res = mod.fit()
    states = pd.DataFrame(np.c_[res.level, res.season, res.resid],
                          columns=['level', 'seasonal', 'resid'], index=res.level.index)
    return (df_resample, states)

def sm_detect_anomalies(df_resample, states, stdev=5):
    """
    Takes in the two dataframes from fit_sm_ExponentialSmoothing()
    and determines if a datapoint is an anomaly. Returns a dataframe.

    Parameters:
        df_resample (pandas dataframe): dataframe we want to analyze
        states (pandas dataframe): name of date/time column
        stdev (int): the amount of standard deviations away the confidence bands should be set
                    * prediction +/- stdev*residual.std()
    Returns:
        anomalies (pandas dataframe): dataframe with resampled value, upper & lower limit,
                                      anomaly (0 false / 1 true), and anomaly importance
    """

    total = states['level'].mean() + states['seasonal']
    resid = df_resample['value'] - total
    upper_limit = total + (stdev * resid.std())
    lower_limit = total - (stdev * resid.std())
    lower_limit = lower_limit.apply(lambda x: max(x, 0))

    df_resample['upper_limit'] = upper_limit
    df_resample['lower_limit'] = lower_limit
    df_resample['anomaly'] = 0
    df_resample.loc[df_resample['value'] > df_resample['upper_limit'], 'anomaly'] = 1
    df_resample.loc[df_resample['value'] < df_resample['lower_limit'], 'anomaly'] = -1

    df_resample['importance'] = 0
    df_resample.loc[df_resample['anomaly'] == 1, 'importance'] = \
        (df_resample['value'] - df_resample['upper_limit']) / df_resample['value']
    df_resample.loc[df_resample['anomaly'] == -1, 'importance'] = \
        (df_resample['lower_limit'] - df_resample['value']) / df_resample['value']
    anomalies = df_resample.copy()
    return anomalies

def sm_plot_anomalies(anomalies, states):
    """
    Takes in dataframe from sm_detect_anomalies()
    and decomposition dataframe from fit_sm_ExponentialSmoothing()
    to plot anomalies if they exist.

     Parameters:
         anomalies (pandas dataframe): dataframe/output from sm_detect_anomalies()
         states (pandas dataframe): dataframe with time series decomposition from fit_sm_ExponentialSmoothing()

    Returns:
        fig (plotly visualization): visualization with true points, upper & lower bands, and anomalies
                                    if they exists (marked in red) as well as time series decomposition
    """
    fig = make_subplots(rows=4, cols=1,
                        subplot_titles=("Level", "Sesonality", "Residual", "Anomaly Detection"))

    fig.add_trace(
        go.Scatter(x=states.index, y=states.level, name='Level'),
        row=1, col=1)

    fig.add_trace(
        go.Scatter(x=states.index, y=states.seasonal, name='Seasonal'),
        row=2, col=1)

    fig.add_trace(
        go.Scatter(x=states.index, y=states.resid, name='Residual'),
        row=3, col=1)

    anoms = anomalies[anomalies.anomaly != 0]

    marker_sizes = []

    for i in list(anoms.importance):
        marker_sizes.append(i * 50)

    fig.add_trace(
        go.Scatter(x=anomalies[anomalies.anomaly == 0].index, y=anomalies[anomalies.anomaly == 0]['value'],
                   mode='markers', opacity=0.5, marker={'color': 'purple'}, name='Metric'),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=anomalies[anomalies.anomaly != 0].index, y=anomalies[anomalies.anomaly != 0]['value'],
                   mode='markers', opacity=0.7, marker={'color': 'red', 'size': marker_sizes}, name='Anomalies'),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=anomalies.index, y=anomalies['upper_limit'].rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Upper Limit'),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=anomalies.index, y=anomalies['lower_limit'].rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Lower Limit'),
        row=4, col=1)

    fig.update_layout(width=1300, height=1300)

    return fig.show()

def plot_importance(pred, date_col):
    """
    Takes in prediction
    and return line plot of anomaly
    importance.

    Parameters:
        pred (pandas dataframe): dataframe from sm_detect_anomalies()
        date_col (str): name of date/time column

    Returns:
        fig (plotly visualization): visualization of anomalies' importance if they exist
    """
    df = pred.copy()
    df.reset_index(inplace=True)
    fig = px.line(df, x=date_col, y="importance",
                  title="Line Chart of Anomaly Importance")
    return fig.show()