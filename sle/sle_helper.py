
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from plots_helper import *


def getfile(cvname="", pdbhost = "pdb-004-production.mistsys.net", overwrite=False, datapath="./"):
    """Retrieve data from pdb continous view. """

    if datapath =="":
        datapath = os.getcwd()
    filename = datapath + "/data-pdb/" + cvname + ".csv"
    print(filename)
    if os.path.isfile(filename) and (not overwrite):
        print(("reading data from file {}".format(filename)))
        df = pd.read_csv(filename)
    else:
        print(("reading data from database and saved to file {}".format(filename)))
        engine = create_engine('postgresql://pipeline@{pdbhost}:5432/pipeline'.format(pdbhost=pdbhost))
        query = 'select * from "{cvname}"'.format(cvname=cvname)
        print((pdbhost, cvname, query))
        df = pd.read_sql_query(query, con=engine)
        df.to_csv(filename)
        
    print(("Time range", df['rt'].min(), df['rt'].max()))
    return df


def getfile_by_site(site, cvname="", pdbhost = "pdb-004-production.mistsys.net", overwrite=False):
    """Retrieve data from pdb continous view for one site """

    engine = create_engine('postgresql://pipeline@{pdbhost}:5432/pipeline'.format(pdbhost=pdbhost))
    query = """select * from "{cvname}" where site_id = '{site}'""".format(cvname=cvname, site=site)
    print((pdbhost, cvname, query))
    df = pd.read_sql_query(query,con=engine)
    #df.to_csv(filename)
        
    print("Time range", df['rt'].min(), df['rt'].max())
    return df


def get_all_data(overwrite=False, datapath='./'):
    cvname = "sle_coverage_trend_by_site_ap_10m" 
    pdbhost = "pdb-004-production.mistsys.net"
    df_trend = getfile(cvname, pdbhost, overwrite, datapath)
    #print df_trend['rt']

    cvname = "sle_coverage_histogram_by_site_ap_10m"
    pdbhost = "pdb-004-production.mistsys.net"
    df_hist = getfile(cvname, pdbhost, overwrite, datapath)

    cvname = "sle_coverage_classifiers_by_client_type_10m"
    pdbhost = "pdb-004-production.mistsys.net"
    df_classifiers = getfile(cvname, pdbhost, overwrite, datapath)
    
    cvname = "sle_coverage_trend_by_site_band_10m" 
    pdbhost = "pdb-004-production.mistsys.net"
    df_band = getfile(cvname, pdbhost, overwrite, datapath)
    #print df_trend['rt']

    cvname = "sle_coverage_histogram_by_site_band_10m"
    pdbhost = "pdb-004-production.mistsys.net"
    df_hist = getfile(cvname, pdbhost, overwrite, datapath)

    return 

#
# dataframe
#

def df_trend_update(df_trend_g, interval = 10.0):
    """Update dataframe, add SLE, SLE_error, anomaly.
    """

    df_trend_g['degraded'].fillna(0, inplace=True)
    df_trend_g['total'].replace(to_replace=0.0, value=1.0,inplace=True)
    
    df_trend_g["sle"] = 1.0 - df_trend_g["degraded"] /df_trend_g["total"]
    # variant of Binomial error, variant of Wilson score interval, N = total_seconds/interval
    df_trend_g["sle_error"] = np.sqrt( (1.0 - df_trend_g["sle"])*df_trend_g["sle"] /df_trend_g['total'] * interval
                                       + (interval/df_trend_g['total'])**2  )

    grandTotal  = df_trend_g['total'].sum()
    grandDegraded =  df_trend_g['degraded'].sum()
    if grandTotal < 1:    grandTotal =1

    m2 = np.sqrt( np.sum(df_trend_g['sle']*df_trend_g['sle']*df_trend_g['total'])/grandTotal )
    m1 = np.sum( df_trend_g['sle']*df_trend_g['total'])/grandTotal
    s1 = np.sqrt( m2**2 - m1**2 )
    print(("avg", m2, m1, "sampling", s1))

    df_trend_g['sle_anomaly'] = (m1 - df_trend_g['sle'])/np.sqrt( s1*s1 + df_trend_g['sle_error'] * df_trend_g['sle_error'])
    df_outlier = df_trend_g[df_trend_g['sle_anomaly']>2.]
    if df_outlier.size > 0.05*df_trend_g.size:  # more outlier
        df_trend_g1 =  df_trend_g[df_trend_g['sle_anomaly']<2.]
        grandTotal1  = df_trend_g1['total'].sum()
        grandDegraded1 =  df_trend_g1['degraded'].sum()
        m2 = np.sqrt( np.sum(df_trend_g1['sle']*df_trend_g1['sle']*df_trend_g1['total'])/grandTotal1 )
        m1 = np.sum(df_trend_g['sle']*df_trend_g1['total'])/grandTotal1
        s1 = np.sqrt(m2**2 - m1**2)

    print(("avg-normal", m2, m1, "sampling", s1))
    df_trend_g['sle_avg'] = m1
    df_trend_g['sle_sampling'] = s1
    df_trend_g['sle_anomaly'] =    (m1 - df_trend_g['sle'])/np.sqrt( s1*s1 + df_trend_g['sle_error']*df_trend_g['sle_error'])
    
    return df_trend_g


def sle_site(df_trend, sitex='0', interval =10, datapath="./"):
    """SLE for sitex
    """
    if sitex and sitex !='0':
        df_trend1 = df_trend[(df_trend['site_id']==sitex)]
    else:
        df_trend1 = df_trend  #for all sites
        sitex = '0'

    print(("site {}  number of total rows={},selected rows={} ".format( sitex, len(df_trend), len(df_trend1) ) ))

    # add datehour into dataframe for hourly operation
    if 'datehour' not in df_trend1.columns:
        df_trend1["datehour"] =  [pd.Timestamp(pd.Timestamp(x).strftime('%Y-%m-%d-%H')) for x in df_trend1['rt']]

    # avoid NAN values
    df_trend['degraded'].fillna(0, inplace=True)
    df_trend['total'].replace(to_replace=0.0, value=1.0, inplace=True)
    df_trend['sle'] = 1-df_trend['degraded']/df_trend['total']

    # group by hour 
    df_trend_g = df_trend1[['datehour', 'total','degraded']].groupby('datehour').sum()
    df_trend_g  = df_trend_update(df_trend_g, interval)
    
    sle_idx = df_trend_g[df_trend_g['sle_anomaly']>3.0].index
    print(("sle_idx", sle_idx.tolist()))
    df_trend_g[df_trend_g['sle_anomaly']>3.0].to_csv("{}/data-out/sle_anomaly_hours_{}.csv".format(datapath, sitex))

    df_trend1['sle_anomaly_hour'] =  [x in sle_idx for x in df_trend1['datehour'] ]
    
    df_trend_x_g = df_trend1[["ap_mac", 'sle_anomaly_hour', 'total','degraded']].groupby(["ap_mac", 'sle_anomaly_hour']).sum()
    df_trend_x_g['sle_ap']= 1 - df_trend_x_g['degraded']/df_trend_x_g['total']
    df_trend_x_g['sle_ap_error']= np.sqrt(df_trend_x_g['sle_ap'] * (1- df_trend_x_g['sle_ap'])/df_trend_x_g['total']*interval
                                          + (interval//df_trend_x_g['total'])**2 )
    df_trend_x_g
    
    return df_trend_g, sle_idx


def plot_sle(df_trend_g, ylim=(0.5, 1.1), sitename='', figname="", datapath="./", sle='coverage'):
    """ plot sle
        -- total vs degraded
        -- sle with errorbar
        -- sle anomaly
    """

    plt.figure(figsize=(10, 25))   
    fig, axes = plt.subplots(nrows=3, ncols=1)

    df_trend_g[[ 'total', 'degraded', 'sle']].plot(x=df_trend_g.index, secondary_y=['sle'],figsize=(10, 10),
                                                  sharex=False, ax=axes[0])

    df_trend_g.plot(x=df_trend_g.index, y='sle',figsize=(10, 10), ylim=ylim, marker='o', ax=axes[1])
    m1 = df_trend_g['sle_avg'].mean()
    s1 = df_trend_g['sle_sampling'].mean()
    print(("testing m1 ", m1, s1))
    axes[1].axhline(m1, color='yellow', lw=2, alpha=0.7)
    print("m1+s1", m1-s1, m1+s1)
    axes[1].fill_between(df_trend_g.index, m1-s1,  m1+s1 , facecolor='yellow',  interpolate=True)
    axes[1].errorbar(df_trend_g.index, df_trend_g['sle'], yerr=df_trend_g['sle_error'])

    #plt.title("{}-SLE".format(sitename))
    
    df_trend_g.plot(y='sle_anomaly',figsize=(10, 10), ylim=(-5.1, 5.1), marker='o', ax=axes[2])
    #plt.axhline( y = 3.0, color='y', ax=axes[2])
    axes[2].axhline( y = 3.0, color='y') 
    plt.title("{} SLE anomaly".format(sitename))

    #plt.hist2d(df_trend_g['sle_anomaly'], df_trend_g['sle_error'],  norm=LogNorm(),bins=(20, 20))
    figname = "{path}/figures/sle_{sle}_{site}.png".format(path=datapath, sle=sle, site=sitename )
    print(("figname:",figname))
    plt.savefig( figname )
    

def plot_sle_hist(df_trend):
    from matplotlib.ticker import FuncFormatter

    # Running the histogram method
    n, bins, patches = plt.hist(df_trend['sle'], 10)

    # To plot correct percentages in the y axis     
    to_percentage = lambda y, pos: str(round( ( y / float(len(df_trend['sle'])) ) * 100.0, 2)) + '%'
    plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percentage))


def getError(p, n):
    """ Binomial Error with small statistics correction"""
    return np.sqrt(p*(1-p)/n + 1/n/n)


def df_trend_anomaly_aps(df_trend_x, sitex, sle_idx=[], interval=10, datapath='./', sle='coverage'):
    """ Given anomaly_time_index (sle_idx), check aps_anomaly
    
    aps, df_trend_x_g = df_trend_anomaly_aps(df_trend[df_trend['site_id']==site6], sle_idx, 10)
    """
    
    if 'datehour' not in df_trend_x.columns:
        df_trend_x["datehour"] =  [pd.Timestamp(pd.Timestamp(x).strftime('%Y-%m-%d-%H')) for x in df_trend_x['rt']]

    df_trend_x['sle_anomaly_hour'] =  [x in sle_idx for x in df_trend_x['datehour'] ]
    df_trend_x_g = df_trend_x[["ap_mac", 'sle_anomaly_hour', 'total','degraded']].groupby(["ap_mac", 'sle_anomaly_hour']).sum()
    df_trend_x_g['sle_ap']= 1 - df_trend_x_g['degraded']/df_trend_x_g['total']
    df_trend_x_g['sle_ap_error']= np.sqrt(df_trend_x_g['sle_ap'] * (1- df_trend_x_g['sle_ap'])/df_trend_x_g['total']*interval
                                         +(interval/df_trend_x_g['total'])**2 )
    #df_trend_x_g
    #df_trend_x_g.unstack(level=1).plot(kind='bar', y=['sle_ap'], subplots=False, figsize=(15, 5))
    df_trend_x_g.unstack(level=1).plot(kind='bar', y='sle_ap',yerr='sle_ap_error', subplots=False, figsize=(15, 5))
    plt.savefig("{path}/figures/aps_anomaly_{site}.png".format(path=datapath, site=sitex ))

    aps = df_trend_x_g.index.get_level_values('ap_mac').unique()
    
    print("ap,       sle_ap_norm, sle_ap_anorm, sle_ap_anomaly_metric, err")
    aps_anomaly=[]
    for apx in aps:
        x =df_trend_x_g.loc[apx]
    
        try:
            xT = x.loc[True]
            xF = x.loc[False]
            if xT['sle_ap']==0.0:
                continue
            diff = xT['sle_ap'] - xF['sle_ap']
            errs = np.sqrt(xT['sle_ap_error']**2 + xF['sle_ap_error']**2 + 0.00001)
            diff = round(1.0*diff/errs, 1)
            #df_trend_x_g.loc[apx]['sle_ap_anomaly'] = diff
      
            if diff< -2:
                print((apx, round(xT['sle_ap'], 3), round(xF['sle_ap'], 3), diff, errs))
                aps_anomaly.append(apx)
        except:
            pass

    print(aps_anomaly)
    df_trend_x_g.to_csv("{path}/data-out/{sle}_aps_anomaly_{site}.csv".format(path=datapath,sle=sle, site=sitex ))
    return aps_anomaly, df_trend_x_g


def df_hist_update(df1, sle_idx=[],  aps_anomaly=[], sitex='',
                   bin_min=-96, bin_width=6.,
                   classifier="coverage", sle_var='coverage', sle_threshold=-70):
    """UPdate dataframe from histogram table
    """

    bin_max = bin_min + bin_width *11
    df1[sle_var] = df1["bucket"]*bin_width + bin_width/2 + bin_min
    df1["dayofweek"] = [pd.Timestamp(x).dayofweek for x in df1['rt']]
    df1["datehour"] = [pd.Timestamp(pd.Timestamp(x).strftime('%Y-%m-%d-%H')) for x in df1['rt']]

    df1[classifier] = df1[sle_var]> sle_threshold
    df1['sle_anomaly'] = [ 1 if x in sle_idx else 0 for x in df1['datehour'] ]

    print((df1['sle_anomaly'].unique()))
    print((df1['datehour'].min(), df1['datehour'].max()))
    print(sle_idx)
    df1.head(2)

    plt.figure(figsize=(15, 5))
    plt.subplot(1, 2, 1)
    plt.hist( df1[df1['sle_anomaly']==0][sle_var], bins=10, range=(bin_min, bin_max))
    plt.xlabel(" normal")
    plt.subplot(1, 2, 2)
    plt.hist( df1[df1['sle_anomaly']>0][sle_var] , bins=10, range=(bin_min, bin_max))
    plt.xlabel(" anomaly")
    #plt.yscale('log', nonposy='clip')

    from scipy.stats import ks_2samp
    from scipy.stats import ttest_ind

    y1=list(df1[df1['sle_anomaly']==0][sle_var])
    y2=list(df1[df1['sle_anomaly']>0][sle_var])

    print(("   ks-test ", " ",  ks_2samp(y1, y2)))
    print(("   t-test  ", " ",   ttest_ind(y1, y2)))

    aps = aps_anomaly
    naps = len(aps)
    print(("naps", naps, aps))

    df1_g = df1[["sle_anomaly", "ap_mac"]].groupby(["ap_mac"]).mean()
    df1_g[df1_g['sle_anomaly']>0.001]
    plt.figure(figsize=(15, 5))
    for i in range(naps):
        apx = aps[i]
        df1x = df1[df1['ap_mac']==apx]

        try:
            plt.subplot(naps/2, 2, i+1)
            plt.hist( df1x[df1x['sle_anomaly']==0][sle_var], normed=True, alpha=0.9, bins=12, range=(-96, -40) )
            #plt.subplot(1, 2, 2)
            plt.hist( df1x[df1x['sle_anomaly']>0][sle_var], normed=True, alpha=0.8,  bins=12, range=(-96, -40)  )
            plt.title(apx)
            y1=list(df1x[df1x['sle_anomaly']==0][sle_var])
            y2=list(df1x[df1x['sle_anomaly']>0][sle_var])
            

            ks= ks_2samp(y2, y1)
            ts = ttest_ind(y2, y1)
            #print apx, len(y1), len(y2)
            #print  " ks-test ", ks
            #print  " t-test ", ts
            #if ts[0]< -2.0:
            plt.title( apx + " t-t="+ str(round(ts.statistic, 2)) + ","+ str(round(ts.pvalue, 3)) + " n2=" + str(len(y2)))
        except:
            pass

    return df1

    
def sle_site_trend_by_band(df_trend, sitex, band='2.4', datapath='./', interval=10):
    """SLE for band"""
    
    print("site",sitex, band, df_trend.size)
    if df_trend.size<1:
        return None, None
    
    df_trend1 = df_trend[df_trend['band']==band]
    df_trend_g = df_trend1[['datehour', 'total','degraded']].groupby('datehour').sum()
    print("df_trend1 size=",band, df_trend1.size)
    
    df_trend_g['degraded'].fillna(0, inplace=True)
    df_trend_g["sle"] = 1 - df_trend_g["degraded"] /df_trend_g["total"]
    df_trend_g["sle_error"] = np.sqrt( (1.0-df_trend_g["sle"])*df_trend_g["sle"] /df_trend_g['total']*interval ) #len(df_trend_g) )
    #df_trend_c = df_trend1[['datehour', 'total']].groupby('datehour').count()
    
    for idx in df_trend_g.index:
        #print idx, df_trend_g.loc[idx]['sle_error'], df_trend_g.loc[idx]['total']
        if df_trend_g.loc[idx]['sle_error']<0.005:
            df_trend_g.loc[idx]['sle_error'] = 1.0/np.sqrt(df_trend_g.loc[idx]['total']) #np.sqrt(df_trend_c.loc[idx]['total'] or 1)
        elif df_trend_g.loc[idx]['sle_error']>0.5:
            df_trend_g.loc[idx]['sle_error'] =0.5

    #df_trend_g["sle_error"] = [ 0.5 if (x<0.0001) else x  for x in df_trend_g["sle_error"]  ]
    #df_trend_g['2017-03-01 00:50:00+00:00':].plot(y='sle', figsize=(15, 10))
    avg_sle = 1-df_trend1["degraded"].sum()/( df_trend1["total"].sum() or 1)
    print("average sle", avg_sle)

    df_trend_g['sle_anomaly'] = df_trend_g['sle']
    for idx in df_trend_g.index:
        errs = df_trend_g.loc[idx]['sle_error']
        if errs< 0.03:
            errs = 0.03

        df_trend_g.loc[idx]['sle_anomaly'] = (avg_sle - df_trend_g.loc[idx]['sle'])/errs
        if df_trend_g.loc[idx]['sle_anomaly'] > 5.0:
            #print idx, df_trend_g.loc[idx]['sle_anomaly'],avg_sle, df_trend_g.loc[idx]['sle'], df_trend_g.loc[idx]['sle_error']
            df_trend_g.loc[idx]['sle_anomaly'] = 5.0 
    
    df_trend_g['sle_anomaly'] = pd.rolling_mean(df_trend_g['sle_anomaly'], 3, center=True)
    df_trend_g.to_csv("{}/data-out/sle_coverage_trend_by_band_{}.csv".format(datapath, sitex))
    
    sle_idx = df_trend_g[df_trend_g['sle_anomaly']>3.0].index
    return df_trend_g, sle_idx



def plot_sle_band(df_trend, sitex, ylim=(0.5, 1.1), datapath='./'):
    df_trend_g2, sle_idx = sle_site_trend_by_band(df_trend, sitex, '2.4', datapath)
    if df_trend_g2 is None:
        return
    df_trend_g2=df_trend_g2.rename(columns={'total':'total_24','degraded':'degraded_24', 'sle':'sle_2', 'sle_anomaly':'sle_anomaly_2'})
    
    df_trend_g5, sle_idx = sle_site_trend_by_band(df_trend, sitex, '5', datapath)
    if df_trend_g5 is None:
        return
    
    df_trend_g5=df_trend_g5.rename(columns={'total':'total_5','degraded':'degraded_5', 'sle':'sle_5', 'sle_anomaly':'sle_anomaly_5'})

    
    #fig, axes = plt.subplots(nrows=1, ncols=2)

    plt.figure(figsize=(15, 25))
    fig, axes = plt.subplots(nrows=3, ncols=1)
    
    ax1 = df_trend_g2.plot(y=['total_24','degraded_24'], figsize=(15, 10), ax=axes[0])
    df_trend_g5.plot(y=['total_5','degraded_5'], figsize=(15, 10), ax=ax1)

    ax = df_trend_g2.plot(y='sle_2', yerr=df_trend_g2['sle_error'], figsize=(15, 10), ylim=ylim, marker='o', ax=axes[1])
    df_trend_g5.plot(y='sle_5', yerr=df_trend_g5['sle_error'], figsize=(15, 10), ylim=ylim, marker='+', ax=ax)
    #plt.errorbar(df_trend_g5.index, df_trend_g5['sle_5'], yerr=df_trend_g5['sle_error'])

    ax1=df_trend_g2.plot(y='sle_anomaly_2',figsize=(15, 10), ylim=(-5.1, 5.1), marker='o', ax=axes[2])
    df_trend_g5.plot(y='sle_anomaly_5',figsize=(15, 10), ylim=(-5.1, 5.1), marker='+', ax=ax1)

    #df_trend_g2.plot(x='sle_error', y='sle_anomaly_2')
    #df_trend_g5.plot(x='sle_error', y='sle_anomaly_5')

    plt.figure(figsize=(15, 10))
    fig, axes = plt.subplots(nrows=1, ncols=2)
 
    df_trend_g2.plot(kind='hexbin', x='sle_error', y='sle_anomaly_2', gridsize=25, title="2.4G", figsize=(10, 5),  ax=axes[0])
    df_trend_g5.plot(kind='hexbin', x='sle_error', y='sle_anomaly_5', gridsize=25, title="5G",  figsize=(10, 5), ax=axes[1])

    plt.savefig("{}/figures/sle_coverage_trend_by_band_{}.png".format(datapath, sitex))
    

#plot_sle_band(site4, (-0.1, 1.))
# import statsmodels.api as sm
# def fit_line2(x, y):
#     """Return slope, intercept of best fit line."""
#     X = sm.add_constant(x)
#     model = sm.OLS(y, X, missing='drop') # ignores entires where x or y is NaN
#     fit = model.fit()
#     return fit.params[1], fit.params[0] # could also return stderr in each via fit.bse





    
import math
def check_correlation( total, degraded, grandTotal, degradedTotal, interval = 10.0, debug=False ):

    goodTotal = grandTotal - degradedTotal
    sigmaF = math.sqrt(1.0*degradedTotal * goodTotal/grandTotal) / grandTotal * interval
    totalF = 1.0*degradedTotal/grandTotal

    d_usage = 1.0* total  / grandTotal
    d_FD    = 1.0* degraded / grandTotal
    d_correlation = degraded/total /totalF
        
    sigmaD  = math.sqrt(1.0* total * (grandTotal - total) /grandTotal ) / grandTotal * interval;
    sigmaFD = math.sqrt(1.0* degraded * (grandTotal - degraded) /grandTotal ) / grandTotal * interval;

    if debug:
        print("     FD", d_FD, "+-" ,  sigmaFD)
        print("     D", d_usage, "+-", sigmaD)
        print("     ", sigmaF/totalF, sigmaD/d_usage, sigmaFD / d_FD)
        print(" ", d_correlation) 

    d_significe = math.pow(sigmaF/totalF, 2)  + \
                  math.pow(sigmaD/d_usage, 2)  + \
                  math.pow(sigmaFD / d_FD, 2)
        
    F_D_correlation = (d_FD - d_usage * totalF)/(d_FD + d_usage * totalF)/2.
    FD_D_correlation = 1.0*degraded/total
    FD_F_correlation = 1.0*degraded/degradedTotal
    if debug:
        print("      correlation-term ", F_D_correlation, FD_D_correlation, FD_F_correlation)

    d_significe = d_significe + \
                  2.0*sigmaF/totalF*sigmaD/d_usage*F_D_correlation - \
                  2.0*sigmaFD/d_FD*sigmaD/d_usage * FD_D_correlation - \
                  2.0*sigmaFD/d_FD*sigmaF/totalF*FD_F_correlation

    d_significe = math.sqrt( d_significe )*d_correlation

    d_impact = (d_correlation - 1.0)/d_significe
    #if d_impact <0:
    #    d_impact = 0
            
    d_correlation = round(d_correlation, 2)
    d_significe  = round(d_significe, 2)
    d_impact = round(d_impact, 2)
        
    if debug:
        print("uncorrelated+correlated ", math.sqrt(d_significe))
        print(" ", d_correlation,  d_significe)
        print("  " "total=", total, " degraded=", degraded, end=' ')
        print("usage=", str(round(d_usage, 4)*100)+"%", " correlation=", d_correlation, "+-", d_significe, " sigificance=", d_impact)
    
    return  d_significe
