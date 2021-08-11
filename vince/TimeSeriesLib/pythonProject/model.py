import pandas as pd
import numpy as np
import re
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import warnings
from statsmodels.tsa.api import ExponentialSmoothing
warnings.filterwarnings("ignore")


# UNIVARIATE FEATURE SELECTION & ANOMALY DETECTION

def remove_known_outliers(df, start_remove, end_remove):
    """Takes in dataframe and starting and ending epoch
    (in ms) of known outliers. Returns dataframe without
    set of continuous outliers."""
    df = df.sort_values(by=['date'])
    start_remove = pd.to_datetime(start_remove,unit='ms')
    end_remove = pd.to_datetime(end_remove,unit='ms')
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df.loc[(df.index < start_remove) | (df.index > end_remove)]

# STATSMODELS EXPONENTIALSMOOTHING

def fit_sm_ExponentialSmoothing(df, metric, resamp_freq):
    """Takes in dataframe,
    metric (string), resample frequency (string).
    Fits and returns two dataframes. One is
    the original dataframne after resampling.
    The other is a dataframe of the data's
    decomposition."""
    df = df[['date', metric]]
    df['date'] = pd.to_datetime(df.date)
    df = df.sort_values(['date'])
    df = df.set_index('date')
    df.columns = ['value']
    df = df.asfreq(freq='T')
    df_resample = df.resample(resamp_freq).mean()
    period = 60 / (int(re.findall("\d+", resamp_freq)[0]))
    # period = number of data points per hour after resampling
    # seasonal_periods = (24 hours in a day)*period+1(make odd number as needed from statsmodels)
    mod = ExponentialSmoothing(df_resample, seasonal_periods=24 * period + 1,
                               trend='add', seasonal='add',
                               use_boxcox=None, initialization_method="estimated",
                               missing='drop')

    res = mod.fit()
    states = pd.DataFrame(np.c_[res.level, res.season, res.resid],
                          columns=['level', 'seasonal', 'resid'], index=df_resample.index)
    return (df_resample, states)

def sm_detect_anomalies(df_resample, states, stdev=5):
    """Takes in the two dataframes from fit_sm_ExponentialSmoothing()
    and determines if a datapoint is an anomaly. Returns a dataframe."""
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
    """Takes in dataframe from sm_detect_anomalies()
    and decomposition dataframe from fit_sm_ExponentialSmoothing()
    to plot anomalies if they exist."""
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