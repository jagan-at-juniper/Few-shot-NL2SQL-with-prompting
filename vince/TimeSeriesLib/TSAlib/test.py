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
import model
import utils

sleroam = pd.read_csv('data/sleroam.csv')
#print(utils.date_range(sleroam))
pred=(model.fit_sm_ExponentialSmoothing(sleroam, ' sle_roaming_v2_impact_dist_by_site_c|aws|eu|flink-lag', '20T'))
anoms = model.sm_detect_anomalies(pred[0], pred[1], stdev=5)
model.sm_plot_anomalies(anoms, pred[1])
print(anoms)

#pred = model.fit_prophet(sleroam, ' sle_roaming_v2_impact_dist_by_site_c|aws|eu|flink-lag', '20T')
#anoms = model.detect_anomalies(pred)
#print(model.plot_anomalies(anoms))

#print(model.plot_proph_importance(anoms))