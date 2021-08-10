import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from signalfx.signalflow import messages
import re

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import warnings
warnings.filterwarnings("ignore")

from statsmodels.tsa.api import ExponentialSmoothing
from fbprophet import Prophet

# DATA LOADING

def get_data_frame(client, program, start, stop, resolution=None):
    """Executes the given program across the given time range (expressed in
    millisecond timestamps since Epoch), and returns a Pandas DataFrame
    containing the results, indexed by output timestamp.
    If the program contains multiple publish() calls, their outputs are merged
    into the returned DataFrame."""
    data = {}
    metadata = {}
    c = client.execute(program, start=start, stop=stop, resolution=resolution)
    for msg in c.stream():
        if isinstance(msg, messages.DataMessage):
            if msg.logical_timestamp_ms in data:
                data[msg.logical_timestamp_ms].update(msg.data)
            else:
                data[msg.logical_timestamp_ms] = msg.data
        elif isinstance(msg, messages.MetadataMessage):
            metadata[msg.tsid] = msg.properties

    df = pd.DataFrame.from_dict(data, orient='index')
    df.metadata = metadata
    columns_names={}
    # renaming columns with topics
    for key in df.metadata:
        if ('provider' in metadata[key]) and ('env' in metadata[key]) and ('topic' in metadata[key]):
            columns_names[key] = df.metadata[key]['topic']+'|'+df.metadata[key]['provider']+'|'+df.metadata[key]['env']
        else:
            columns_names[key] = df.metadata[key]['topic']+'|'+df.metadata[key]['env']
    df = df.rename(columns=columns_names)
    df = df.dropna(axis=1,how='all')
    reset_df = df.reset_index()
    reset_df['date']=pd.to_datetime(reset_df['index'],unit='ms')
    reset_df = reset_df.drop(columns=['index'])
    reset_df = reset_df.set_index('date')
    return reset_df.loc[:, reset_df.sum().astype(bool)]

def get_papi_data_frame(client, program, start, stop, resolution=None):
    """Executes the given program across the given time range (expressed in
    millisecond timestamps since Epoch), and returns a Pandas DataFrame
    containing the results, indexed by output timestamp.
    If the program contains multiple publish() calls, their outputs are merged
    into the returned DataFrame."""
    data = {}
    metadata = {}
    c = client.execute(program, start=start, stop=stop, resolution=resolution)
    for msg in c.stream():
        if isinstance(msg, messages.DataMessage):
            if msg.logical_timestamp_ms in data:
                data[msg.logical_timestamp_ms].update(msg.data)
            else:
                data[msg.logical_timestamp_ms] = msg.data
        elif isinstance(msg, messages.MetadataMessage):
            metadata[msg.tsid] = msg.properties

    df = pd.DataFrame.from_dict(data, orient='index')
    df.metadata = metadata
    columns_names={}
    # renaming columns with topics
    for key in df.metadata:
        if ('provider' in metadata[key]) and ('env' in metadata[key]) and ('app' in metadata[key]) and ('service' in metadata[key]):
            columns_names[key] = df.metadata[key]['app']+'|'+df.metadata[key]['provider']+'|'+df.metadata[key]['env']+'|'+df.metadata[key]['service']
        else:
            columns_names[key] = df.metadata[key]['app']+'|'+df.metadata[key]['provider']+'|'+df.metadata[key]['env']
    df = df.rename(columns=columns_names)
    df = df.dropna(axis=1,how='all')
    reset_df = df.reset_index()
    reset_df['date']=pd.to_datetime(reset_df['index'],unit='ms')
    reset_df = reset_df.drop(columns=['index'])
    reset_df = reset_df.set_index('date')
    return reset_df.loc[:, reset_df.sum().astype(bool)]

def unique_metadata_len(client, program, start, stop, resolution=None):
    """Returns unique lengths of metadata for requested program. Used to provide
    more detailsabout what keys exist and missing."""
    metadata = {}
    c = client.execute(program, start=start, stop=stop, resolution=resolution)
    for msg in c.stream():
        if isinstance(msg, messages.MetadataMessage):
            metadata[msg.tsid] = msg.properties
    keys = list(metadata)
    lenlst = []
    keylst=[]
    for i in keys:
        lenlst.append(len(metadata[i]))
        keylst.append(metadata[i]['sf_key'])
    res = pd.DataFrame()
    res['key']=keys
    res['key_length']=lenlst
    res['available_keys']=keylst
    res = res.set_index('key')
    res = res.drop_duplicates(subset=['key_length'])
    return res

# DATA QUALITY CHECKS

def date_range(df):
    """Return range of given
    dataframe"""
    df=df.reset_index()
    lower= (min(df.date))
    upper= (max(df.date))
    return (lower,upper)

def missing_data(df):
    """Returns the proportion
    of missing data per column"""
    res = pd.DataFrame(df.isna().sum())
    res.columns = ['nulls']
    res = res.sort_values(by=['nulls'],ascending=False)
    return res/len(df)

def check_entries(df):
    """Count negative and
    zero entries"""
    df = df.set_index('date')
    negs = pd.DataFrame(df.where(df<0).count())
    zeros = pd.DataFrame(df.where(df==0).count())
    res = pd.concat([negs, zeros], axis=1)
    res.columns=['negatives','zeroes']
    return res

# DATA QUERYING

def filter_df(df, filter_word):
    """Returns dataframe after
    refined column names"""
    a_list = df.columns
    str2check ="^.*"+filter_word+".*$"
    r = re.compile(str2check)
    filtered_list = list(filter(r.match, a_list))
    return df[filtered_list]

def plot_ts(df, metric, color=None):
    """Takes in dataframe with
    epoch time as indices and generates
    time series plot with one
    metric"""
    reset_df = df.reset_index()
    reset_df = reset_df.sort_values(by=['date'])
    plt.rc('font', size=12)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlabel('Time')
    ax.set_ylabel('Stats')
    ax.set_title(metric + ' Time Series')
    ax.grid(True)
    return ax.plot(reset_df.date, reset_df[metric], color=None)

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

def sm_ExponentialSmoothing(df, metric, resamp_freq, stdev=5):
    """Takes in dataframe,
    metric (string), resample frequency (string).
    Fits and returns time series
    decomposition using
    StatsModels Exponential Smoothing
    model"""
    orig = df.copy()
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



    num_anoms = len(df_resample[df_resample.anomaly != 0])
    anoms = df_resample[df_resample.anomaly != 0]

    marker_sizes = []
    start = 30
    for i in list(anoms.importance):
        marker_sizes.append(i * 50)

    fig.add_trace(
        go.Scatter(x=df_resample[df_resample.anomaly == 0].index, y=df_resample[df_resample.anomaly == 0]['value'],
                   mode='markers', opacity=0.5, marker={'color': 'purple'}, name=metric),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=df_resample[df_resample.anomaly != 0].index, y=df_resample[df_resample.anomaly != 0]['value'],
                   mode='markers', opacity=0.7, marker={'color': 'red', 'size': marker_sizes}, name='Anomalies'),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=df_resample.index, y=upper_limit.rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Upper Limit'),
        row=4, col=1)

    fig.add_trace(
        go.Scatter(x=df_resample.index, y=lower_limit.rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Lower Limit'),
        row=4, col=1)

    fig.update_layout(width=1300, height=1300)

    return fig.show()

# PROPHET

def fit_prophet(df, metric, resamp_freq, interval_width=.99, changepoint_range=.01):
    """Takes in dataframe, metric/column name(str),
     resampling frequency ('20T' = 20 min). Interval
     width and change point range set for maximum band
     width. Fits the data and returns dataframe of
     predictions (yhat upper & lower)"""
    df = df[['date', metric]]
    df['date'] = pd.to_datetime(df.date)
    df = df.sort_values(['date'])
    df = df.set_index('date')
    df.columns = ['value']
    df = df.asfreq(freq='T')
    df_resample = df.resample(resamp_freq).mean()
    df_resample = df_resample.reset_index()
    df_resample.columns = ['ds', 'y']
    m = Prophet(daily_seasonality=True, yearly_seasonality=False, weekly_seasonality=True,
                seasonality_mode='additive',
                interval_width=interval_width,
                changepoint_range=changepoint_range)

    m = m.fit(df_resample)

    forecast = m.predict(df_resample)
    forecast['fact'] = df_resample['y'].reset_index(drop=True)
    fig1 = m.plot(forecast)
    return forecast


def detect_anomalies(forecast):
    """Takes in dataframe from
    fit_prophet. Determines if
    data point is an anomaly.
    Returns a dataframe."""
    forecasted = forecast[['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()

    forecasted['yhat_lower'] = forecasted['yhat_lower'].apply(lambda x: max(x, 0))

    forecasted['anomaly'] = 0
    forecasted.loc[forecasted['fact'] > forecasted['yhat_upper'], 'anomaly'] = 1
    forecasted.loc[forecasted['fact'] < forecasted['yhat_lower'], 'anomaly'] = -1

    # anomaly importances
    forecasted['importance'] = 0
    forecasted.loc[forecasted['anomaly'] == 1, 'importance'] = \
        (forecasted['fact'] - forecasted['yhat_upper']) / forecast['fact']
    forecasted.loc[forecasted['anomaly'] == -1, 'importance'] = \
        (forecasted['yhat_lower'] - forecasted['fact']) / forecast['fact']
    return forecasted


def plot_anomalies(forecasted):
    """Takes in output from
    detect_anomalies. Returns
    visualization of outliers."""
    fig = make_subplots(rows=1, cols=1)
    fig.update_layout(title_text="Anomaly Detection",
                      title_font_size=30)

    num_anoms = len(forecasted[forecasted.anomaly != 0])
    anoms = forecasted[forecasted.anomaly != 0]
    marker_sizes = []
    start = 30
    for i in list(anoms.importance):
        marker_sizes.append(i * 50)

    fig.add_trace(
        go.Scatter(x=forecasted[forecasted.anomaly == 0].ds, y=forecasted[forecasted.anomaly == 0]['fact'],
                   mode='markers', opacity=0.5, marker={'color': 'purple'}, name='Metric'),
        row=1, col=1)

    fig.add_trace(
        go.Scatter(x=forecasted[forecasted.anomaly != 0].ds, y=forecasted[forecasted.anomaly != 0]['fact'],
                   mode='markers', opacity=0.7,
                   marker={'color': 'red', 'size': forecasted[forecasted.anomaly != 0].importance * 25},
                   name='Anomalies'),
        row=1, col=1)

    fig.add_trace(
        go.Scatter(x=forecasted.ds,
                   y=forecasted['yhat_upper'].rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Upper Limit'),
        row=1, col=1)

    fig.add_trace(
        go.Scatter(x=forecasted.ds,
                   y=forecasted['yhat_lower'].rolling(window=5, win_type='gaussian', center=True).mean(std=1.0),
                   name='Lower Limit'),
        row=1, col=1)

    fig.update_layout(width=1000, height=600)

    return fig.show()

def plot_proph_importance(proph_pred):
    """Takes in Prophet prediction
    and return line plot of anomaly
    importance"""
    fig = px.line(proph_pred, x="ds", y="importance",
                  title="Line Chart of Anomaly Importance")
    return fig.show()