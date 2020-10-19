import pandas as pd

from enum import Enum

class SnapchatContext(Enum):
  campaign_id = 'campaign_id'

def convert_time_series_to_utc(series: pd.Series):
  return pd.to_datetime(series).dt.tz_convert(tz='UTC').dt.tz_localize(tz=None)
