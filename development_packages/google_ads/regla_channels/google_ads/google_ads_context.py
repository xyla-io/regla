import pandas as pd

from enum import Enum
from regla import RuleContextOption

class GoogleAdsContext(Enum):
  campaign_id = 'campaign_id'

class GoogleAdsOption(RuleContextOption, Enum):
  use_optimized_conversions = 'use_optimized_conversions'
  wait_days = 'wait_days'
  wait_conversions = 'wait_conversions'
  wait_optimized_conversions = 'wait_optimized_conversions'

  @property
  def default(self) -> any:
    if self is GoogleAdsOption.use_optimized_conversions:
      return True
    elif self is GoogleAdsOption.wait_days:
      return 1
    elif self is GoogleAdsOption.wait_conversions:
      return 100
    elif self is GoogleAdsOption.wait_optimized_conversions:
      return 10
    else:
      raise ValueError('Unsupported Google Ads option', self)

class GoogleAdsColumn(Enum):
  last_adjustment_time = 'last_adjustment_time'
  last_adjustment_description = 'last_adjustment_description'
  wait_metrics_since_time = 'wait_metrics_since_time'
  wait_conversions = 'wait_conversions'
  wait_optimized_conversions = 'wait_optimized_conversions'

def add_report_time(report: pd.DataFrame, time_column: str='time'):
  if report.empty:
    return
  report[time_column] = report['segments_date']
  if 'segments_hour' in report.columns:
    report[time_column] = report[time_column] + ' ' + report.segments_hour.apply(lambda h: f'{h}:00')
  report[time_column] = pd.to_datetime(report[time_column])
  assert len(report.customer_time_zone.unique()) == 1
  time_zone = report.customer_time_zone.iloc[0]
  report[time_column] = report[time_column].dt.tz_localize(tz=time_zone, ambiguous='infer').dt.tz_convert(tz='UTC').dt.tz_localize(tz=None)
