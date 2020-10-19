import pandas as pd

from bson import ObjectId
from typing import Dict, Optional, TypeVar, Generic
from .context_models import RuleContext
from moda.connect import Connector
from .action_models import RuleAction
from .action_types import RuleActionType
from .report_models import RuleReporter, RuleReportType, RuleReportGranularity
from datetime import datetime
from enum import Enum
from typing import List, Dict

class ChannelEntity(Enum):
  searchterm = 'searchterm'
  keyword = 'keyword'
  ad = 'ad'
  ad_group = 'ad_group'
  campaign = 'campaign'
  org = 'org'

A = TypeVar(any)
C = TypeVar(any)
class Channel(Generic[A, C], Connector):
  options: Dict[str, any]
  api: Optional[A]=None

  def __init__(self, options: Dict[str, any]={}):
    self.options = {**options}

  @property
  def identifier(self) -> str:
    raise NotImplementedError()

  @property
  def title(self) -> str:
    raise NotImplementedError()

  def disconnect(self):
    self.api = None

  def rule_context(self, options: Dict[str, any]={}) -> C:
    raise NotImplementedError()

  def report_type(self, action_type: RuleActionType) -> RuleReportType:
    raise NotImplementedError()

  def rule_reporter(self, report_type: Optional[RuleReportType]=None, ad_group_id: Optional[str]=None, rule_id: Optional[ObjectId]=None, data_check_range: Optional[int]=None, raw_report: Optional[pd.DataFrame]=None, report:Optional[pd.DataFrame]=None) -> RuleReporter:
    raise NotImplementedError()

  def rule_action(self, action_type: RuleActionType, adjustment_value: Optional[float]=None, adjustment_limit: Optional[float]=None) -> RuleAction:
    raise NotImplementedError()

  def get_entities(self, entity_type: ChannelEntity, parent_ids: Dict[ChannelEntity, str]={}) -> List[Dict[str, any]]:
    raise NotImplementedError()

  def granularity_is_compatible(self, granularity: RuleReportGranularity, report_type: RuleReportType, start_date: datetime, end_date: datetime):
    return True

  def highest_compatible_granularity(self, report_type: RuleReportType, start_date: datetime, end_date: datetime):
    for g in RuleReportGranularity:
      if self.granularity_is_compatible(
        granularity=g,
        report_type=report_type,
        start_date=start_date,
        end_date=end_date
      ):
        return g
    return None