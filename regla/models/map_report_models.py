import pandas as pd

from typing import Optional, List, Dict, Type
from datetime import datetime
from io_map import IOMap, IOMapKey
from .report_models import RuleReporter
from .context_models import RuleContext, RuleOption

class MapReporter(RuleReporter, IOMap):
  raw_report: Optional[pd.DataFrame]=None
  report: Optional[pd.DataFrame]=None

  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'rule_reporter'

  @classmethod
  def get_calcuate_keys(cls) -> List[str]:
    return [
      'raw_reporter_class',
      'raw_columns',
      'raw_options',
    ]

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return [
      'raw_report',
      'report',
    ]

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: 'calculate.raw_reporter_class',
        IOMapKey.construct.value: {
          'columns': 'calculate.raw_columns',
          'options': 'calculate.raw_options',
        },
        IOMapKey.input.value: {
          'start_date': 'input.start_date',
          'end_date': 'input.end_date',
          'granularity': 'input.granularity',
          'api': 'input.api',
          'context': 'input.context',
        },
        IOMapKey.output.value: {
          'raw_report': 'output.raw_report',
        }
      }
    ]

  @property
  def raw_reporter_class(self) -> Type[IOMap]:
    return RawReporter

  @property
  def raw_columns(self) -> List[str]:
    return []

  @property
  def raw_options(self) -> Dict[str, any]:
    return {}

  def run(self, start_date: datetime, end_date: datetime, granularity: str, api: any, context: any):
    self.start_date = start_date
    self.end_date = end_date
    self.granularity = granularity
    self.api = api
    self.context = context
    self.prepare_maps()
    self.run_maps()
    return self.populated_output

  def _getRawReport(self, startDate, endDate, granularity, api, campaign, adGroupIDs):
    return self.run(
      start_date=startDate,
      end_date=endDate,
      granularity=granularity,
      api=api,
      context=campaign
    )['raw_report']

  def _filterByLastActionDate(self, report, historyCollection):
    if not self.context[RuleContext.rule_options.value][RuleOption.dynamic_window.value]:
      return
    return super()._filterByLastActionDate(
      report=report,
      historyCollection=historyCollection
    )

class RawReporter(IOMap):
  api: Optional[any]=None
  context: Optional[any]=None
  start_date: Optional[datetime]=None
  end_date: Optional[datetime]=None
  raw_report: Optional[pd.DataFrame]=None
  columns: List[str]
  options: Dict[str, any]

  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'raw_reporter'

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return ['raw_report']

  def __init__(self, columns: List[str], options: Dict[str, any]={}):
    self.columns = [*columns]
    self.options = {**options}

  def run(self, start_date: Optional[datetime], end_date: Optional[datetime], granularity: str, api: any, context: any) -> Dict[str, any]:
    self.start_date = start_date
    self.end_date = end_date
    self.granularity = granularity
    self.api = api
    self.context = context
    self.raw_report = self.fetch_raw_report()
    return self.populated_output

  def fetch_raw_report(self) -> pd.DataFrame:
    return pd.DataFrame()

  