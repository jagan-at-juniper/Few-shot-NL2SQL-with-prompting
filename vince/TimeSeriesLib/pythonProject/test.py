import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from signalfx.signalflow import messages
import re

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import warnings
warnings.filterwarnings("ignore")

from statsmodels.tsa.api import ExponentialSmoothing
from fbprophet import Prophet
import main

sleroam = pd.read_csv('data/sleroam.csv')
#print(main.date_range(sleroam))
print(main.sm_ExponentialSmoothing(sleroam,' sle_roaming_v2_impact_dist_by_site_c|aws|eu|flink-lag', '20T'))

pred = main.fit_prophet(sleroam,' sle_roaming_v2_impact_dist_by_site_c|aws|eu|flink-lag', '20T')
anoms = main.detect_anomalies(pred)
#print(main.plot_anomalies(anoms))

#print(main.plot_proph_importance(anoms))