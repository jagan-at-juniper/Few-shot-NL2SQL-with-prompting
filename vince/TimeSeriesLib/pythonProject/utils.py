import pandas as pd
import matplotlib.pyplot as plt
from signalfx.signalflow import messages
import re
import warnings
warnings.filterwarnings("ignore")


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
