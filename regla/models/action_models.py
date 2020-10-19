import json
import bson
import traceback
import pandas as pd

from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Callable
from functools import total_ordering
from .context_models import RuleContext, RuleOption
from .report_models import RuleReportColumn
from .action_types import RuleActionType
from ..errors import RuleActionMissingTargetError, RuleActionEntityError

class RuleActionResult:
    def __init__(self,
                 apiResponse=None,
                 report=None,
                 dryRun=None,
                 errors=None,
                 logs=None,
                 action_report=None):
        self.report = report
        self.apiResponse = apiResponse
        self.dryRun = dryRun
        self.errors = [] if errors is None else errors
        self.logs = [] if logs is None else logs
        self.action_report = action_report

    def serialize_result(self):
        return {
            "report": self.report.to_csv() if self.report is not None and not self.report.empty else "",
            "action_report": self.action_report.to_csv() if self.action_report is not None and not self.action_report.empty else "",
            "apiResponse": self.apiResponse,
            "logs": self.logs,
            "dryRun": self.dryRun,
            "errors": [repr(e) if e is not None else None for e in self.errors],
        }

@total_ordering
class RuleActionPreference(Enum):
  make_adjustment = 'make_adjustment'
  modify_adjustment = 'modify_adjustment'
  prevent_adjustment = 'prevent_adjustment'

  def __lt__(self, other):
    if self.__class__ is not other.__class__:
      return NotImplemented
    cases = list(self.__class__)
    return cases.index(self) < cases.index(other)

class RuleActionReportColumn(Enum):
  target_id = 'target_id'
  target_name = 'target_name'
  history = 'history'
  adjustment = 'adjustment'
  preference = 'preference'
  override_preference = 'override_preference'
  preference_messages = 'preference_messages'
  preferred_adjustment = 'preferred_adjustment'
  unpreferred_adjustment = 'unpreferred_adjustment'
  prehistoric_state = 'prehistoric_state'
  unadjusted_state = 'unadjusted_state'
  log = 'log'
  error = 'error'
  api_request = 'api_request'
  api_response = 'api_response'
  dry_run = 'dry_run'

class RuleActionTargetType(Enum):
  campaign = 'campaign'
  keyword = 'keyword'
  adgroup = 'adgroup'

class RuleActionAdjustmentType(Enum):
  status = 'status'
  budget = 'budget'
  cpa_goal = 'cpa_goal'
  no_action = 'no_action'

class RuleActionLog:
  targetID: any
  targetType: Optional[RuleActionTargetType]
  targetChannel: Optional[str]
  adjustmentType: Optional[RuleActionAdjustmentType]
  adjustmentFrom: Optional[any]
  adjustmentTo: Optional[any]
  targetDescription: Optional[str]
  actionDescription: str
  consumedData: bool

  def __init__(self,
    targetID=None,
    targetType=None,
    targetChannel=None,
    adjustmentType=None,
    adjustmentFrom=None,
    adjustmentTo=None,
    targetDescription=None,
    actionDescription=None,
    consumedData=True):
    self.targetID = targetID
    self.targetType = targetType
    self.targetChannel = targetChannel
    self.adjustmentType = adjustmentType
    self.adjustmentFrom = adjustmentFrom
    self.adjustmentTo=adjustmentTo
    self.targetDescription = targetDescription
    self.actionDescription = actionDescription
    self.consumedData = consumedData

  @property
  def dbRepresentation(self):
    return {
      'historyType': 'action',
      'actionCount': 1,
      'targetID': self.targetID,
      'targetType': self.targetType.value,
      'targetChannel': self.targetChannel,
      'adjustmentType': self.adjustmentType.value if self.adjustmentType is not None else None,
      'adjustmentFrom': self.adjustmentFrom,
      'adjustmentTo': self.adjustmentTo,
      'targetDescription': self.targetDescription,
      'actionDescription': self.actionDescription,
      'consumedData': self.consumedData,
    }

  def __str__(self):
    return str(vars(self))

  def serialize_result(self):
    return {
      'targetID': self.targetID,
      'targetType': self.targetType.value,
      'targetChannel': self.targetChannel,
      'adjustmentType': self.adjustmentType.value if self.adjustmentType is not None else None,
      'adjustmentFrom': self.adjustmentFrom,
      'adjustmentTo': self.adjustmentTo,
      'targetDescription': self.targetDescription,
      'actionDescription': self.actionDescription,
      'consumedData': self.consumedData,
    }

class RuleAction:
  type: Optional[RuleActionType]
  adjustmentValue: Optional[any]
  adjustmentLimit: Optional[any]

  def __init__(self, type=None, adjustmentValue=None, adjustmentLimit=None):
    self.type = type
    self.adjustmentValue = adjustmentValue
    self.adjustmentLimit = adjustmentLimit

  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    raise NotImplementedError()

  @property
  def entity_granularity(self) -> RuleActionTargetType:
    raise NotImplementedError()

  @property 
  def preferences_title(self) -> str:
    return 'channel preferences'

  @property
  def action_report_columns(self) -> List[str]:
    return [c.value for c in RuleActionReportColumn]

  def adjust(self, api: any, campaign: any, report: pd.DataFrame, dryRun: bool) -> RuleActionResult:
    if report.empty:
      return RuleActionResult(
        report=report,
        dryRun=dryRun
      )

    action_report = self.generate_action_report(
      api=api,
      report=report,
      dry_run=dryRun,
      context=campaign
    )
    action_report = self.interpret_action_report(
      api=api,
      action_report=action_report,
      context=campaign
    )
    self.execute_action_report(
      api=api,
      action_report=action_report,
      context=campaign
    )

    return RuleActionResult(
      apiResponse=list(action_report[RuleActionReportColumn.api_response.value]),
      report=report,
      action_report=action_report,
      dryRun=dryRun,
      errors=list(action_report[RuleActionReportColumn.error.value]),
      logs=list(action_report[RuleActionReportColumn.log.value])
    )

  def entity_apply(self, action_report: pd.DataFrame, transformer: Callable[[pd.Series, any], Optional[pd.Series]], location: Optional[pd.DataFrame]=None):
    if location is None:
      location = action_report
    if location.empty:
      return

    def apply_transformer(entity_series: pd.Series):
      try:
        series = transformer(entity_series)
        if series is not None:
          return series
      except (KeyboardInterrupt, SystemExit):
        raise
      except Exception as e:
        entity_series[RuleActionReportColumn.error.value] = RuleActionEntityError(
          target_id=entity_series[RuleActionReportColumn.target_id.value],
          error=e,
          traceback=traceback.format_exc()
        )
      return entity_series

    # Using location.apply(apply_transformer, axis='columns') has the unexpected side-effect of calling apply_transformer on the first row twice under certain circumstances (see https://stackoverflow.com/questions/21635915/why-does-pandas-apply-calculate-twice)
    # Using pd.DataFrame([apply_transformer(location.loc[i].copy()) for i in location.index], index=location.index) unexpectedly converts types
    for index in location.index:
      action_report.loc[index, :] = apply_transformer(location.loc[index].copy())

  #-------------------------------------
  # Generate Action Report
  #-------------------------------------
  def generate_action_report(self, api: any, report: pd.DataFrame, dry_run: bool, context: any) -> pd.DataFrame:
    entity_ids = self.get_entity_ids(
      report=report,
      context=context
    )
    action_report = self.get_raw_action_report(
      entity_ids=entity_ids,
      api=api,
      report=report,
      context=context
    )
    self.map_action_report(
      action_report=action_report,
      context=context
    )
    action_report = self.shape_action_report(
      entity_ids=entity_ids,
      action_report=action_report,
      dry_run=dry_run,
      context=context
    )
    self.add_action_report_history(
      entity_ids=entity_ids,
      action_report=action_report,
      context=context
    )
    action_report = self.supplement_action_report(
      action_report=action_report,
      api=api,
      context=context
    )
    return action_report

  def get_entity_ids(self, report: pd.DataFrame, context: any) -> List[str]:
    def report_entity_id_column(granularity) -> str:
      if granularity is RuleActionTargetType.campaign:
        return RuleReportColumn.campaign_id.value
      elif granularity is RuleActionTargetType.adgroup:
        return RuleReportColumn.ad_group_id.value
      else:
        raise ValueError('Unsupported entity granularity', granularity)
    entity_id_column = report_entity_id_column(granularity=self.entity_granularity)
    entity_ids = list(map(str, report[entity_id_column].unique()))
    return entity_ids

  def get_raw_action_report(self, entity_ids: List[str], api: any, report: pd.DataFrame, context: any) -> pd.DataFrame:
    raise NotImplementedError()

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    pass

  def shape_action_report(self, entity_ids: List[str], action_report: pd.DataFrame, dry_run: bool, context: any) -> pd.DataFrame:
    action_report = action_report.append([
      {RuleActionReportColumn.target_id.value: i, RuleActionReportColumn.error.value: RuleActionMissingTargetError(target_id=i)}
      for i in entity_ids
      if i not in action_report[RuleActionReportColumn.target_id.value].values
    ])
    assert not action_report[RuleActionReportColumn.target_id.value].duplicated().any(), 'Duplicate entity IDs in action report'
    assert action_report[RuleActionReportColumn.target_id.value].isna().unique() == [False], 'N/A entity IDs in action report'

    action_report[RuleActionReportColumn.preference.value] = RuleActionPreference.make_adjustment
    action_report[RuleActionReportColumn.override_preference.value] = not context[RuleContext.rule.value].safe_mode
    action_report[RuleActionReportColumn.preference_messages.value] = [[] for _ in range(len(action_report))]
    action_report[RuleActionReportColumn.dry_run.value] = dry_run
    for column in self.action_report_columns:
      if column not in action_report.columns:
        action_report[column] = None
    return action_report

  def add_action_report_history(self, entity_ids: List[str], action_report: pd.DataFrame, context: any):
    history = self.get_entity_history(
      entity_ids=action_report[RuleActionReportColumn.target_id.value].tolist(),
      context=context
    )
    def add_history(entity_series: pd.Series):
      entity_history = sorted(filter(lambda h: 'targetID' in h and str(h['targetID']) == entity_series[RuleActionReportColumn.target_id.value], history), key=lambda h: h['historyCreationDate'])
      entity_series[RuleActionReportColumn.history.value] = entity_history
    self.entity_apply(
      action_report=action_report,
      transformer=add_history
    )

  def get_entity_history(self, entity_ids: List[str], context: any) -> List[Dict[str, any]]:
    user_id = context[RuleContext.rule.value].userID
    channel_identifier = context[RuleContext.channel.value].identifier
    history_collection = context[RuleContext.history_collection.value]

    target_ids = [self.entity_target_id(entity_id=i) for i in entity_ids]
    history_conditions = {
      'userID': bson.ObjectId(user_id),
      'targetChannel': channel_identifier,
      'targetID': {'$in': target_ids},
      'consumedData': True,
    }
    if not context[RuleContext.rule_options.value][RuleOption.use_dry_run_history.value]:
      history_conditions['dryRun'] = False
    entity_history = list(history_collection.find(history_conditions).sort('historyCreationDate'))
    return entity_history

  def supplement_action_report(self, action_report: pd.DataFrame, api: any, context: any) -> pd.DataFrame:
    return action_report

  def entity_target_id(self, entity_id: str) -> any:
    return int(entity_id)

  #-------------------------------------
  # Interpret Action Report
  #-------------------------------------
  def interpret_action_report(self, api: any, action_report: pd.DataFrame, context: any) -> pd.DataFrame:
    self.add_action_report_adjustments(
      action_report=action_report,
      api=api,
      context=context
    )
    self.set_action_report_preferences(
      action_report=action_report,
      api=api,
      context=context
    )
    action_report = self.transform_action_report(
      action_report=action_report,
      api=api,
      context=context
    )
    return action_report

  def add_action_report_adjustments(self, action_report: pd.DataFrame, api: any, context: any):
    def add_ajustment(entity_series: pd.Series):
      entity_series[RuleActionReportColumn.adjustment.value] = self.entity_adjustment(
      entity_series=entity_series,
      context=context
    )
    location = action_report.loc[action_report[RuleActionReportColumn.error.value].isna()]
    self.entity_apply(
      action_report=action_report,
      transformer=add_ajustment,
      location=location
    )

  def entity_adjustment(self, entity_series: pd.Series, context: any) -> Optional[any]:
    raise NotImplementedError()

  def set_action_report_preferences(self, action_report: pd.DataFrame, api: any, context: any):
    pass
  
  def add_preference(self, action_report: pd.DataFrame, location: pd.DataFrame, preference: RuleActionPreference, message_callback: Callable[[any], str]):
    def update_preference(entity_series: pd.Series):
      entity_series[RuleActionReportColumn.preference.value] = max(entity_series[RuleActionReportColumn.preference.value], preference)
      entity_series[RuleActionReportColumn.preference_messages.value].append(message_callback(entity_series))
    self.entity_apply(
      action_report=action_report,
      transformer=update_preference,
      location=location
    )

  def transform_action_report(self, action_report: pd.DataFrame, api: any, context: any) -> pd.DataFrame:
    return action_report

  #-------------------------------------
  # Execute Action Report
  #-------------------------------------
  def execute_action_report(self, api: any, action_report: pd.DataFrame, context: any) -> pd.DataFrame:
    self.add_action_report_logs(
      action_report=action_report,
      api=api,
      context=context
    )
    self.add_action_report_requests(
      action_report=action_report,
      api=api,
      context=context
    )
    self.commit_action_report_requests(
      action_report=action_report,
      api=api,
      context=context
    )
    action_report = self.finalize_action_report(
      action_report=action_report,
      api=api,
      context=context
    )
    return action_report

  def add_action_report_logs(self, api: any, action_report: pd.DataFrame, context: any):
    def add_log(entity_series: pd.Series):
      entity_series[RuleActionReportColumn.log.value] = self.entity_log(
        entity_series=entity_series,
        context=context
      )
    location = action_report.loc[(action_report[RuleActionReportColumn.error.value].isna()) & (action_report[RuleActionReportColumn.adjustment.value].notna())]
    self.entity_apply(
      action_report=action_report,
      transformer=add_log,
      location=location
    )

  def entity_log(self, entity_series: pd.Series, context: any) -> Optional[RuleActionLog]:
    action_description = self.entity_action_description(
      entity_series=entity_series,
      context=context
    )
    if action_description is None:
      return None
    log = RuleActionLog(
      targetID=self.entity_target_id(entity_series[RuleActionReportColumn.target_id.value]),
      targetType=self.entity_granularity,
      targetChannel=context[RuleContext.channel.value].identifier,
      targetDescription=entity_series[RuleActionReportColumn.target_name.value],
      adjustmentType=self.adjustment_type,
      adjustmentFrom=entity_series[RuleActionReportColumn.unadjusted_state.value],
      adjustmentTo=entity_series[RuleActionReportColumn.adjustment.value]
    )
    log.actionDescription = self.entity_action_description(
      entity_series=entity_series,
      context=context
    )
    log.consumedData = bool(entity_series[RuleActionReportColumn.override_preference.value]) or entity_series[RuleActionReportColumn.preference.value] is not RuleActionPreference.prevent_adjustment
    return log

  def entity_action_description(self, entity_series: pd.Series, context: any) -> str:
    base_action_description = self.action_description(
      entity_series=entity_series,
      context=context
    )
    if base_action_description is None:
      return None
    if entity_series[RuleActionReportColumn.preference.value] is RuleActionPreference.make_adjustment:
      return base_action_description

    if entity_series[RuleActionReportColumn.override_preference.value]:
      action_prefix = ''
      preference_treatment = 'overriding'
    elif entity_series[RuleActionReportColumn.preference.value] is RuleActionPreference.modify_adjustment:
      action_prefix = 'moderately '
      preference_treatment = 'using'
    elif entity_series[RuleActionReportColumn.preference.value] is RuleActionPreference.prevent_adjustment:
      action_prefix = 'should have '
      preference_treatment = 'but did not due to'
    return f'{action_prefix}{base_action_description} {preference_treatment} {self.preferences_title}: {", ".join(entity_series[RuleActionReportColumn.preference_messages.value])}'

  def action_description(self, entity_series: pd.Series, context: any):
    raise NotImplementedError()

  def add_action_report_requests(self, api: any, action_report: pd.DataFrame, context: any):
    def add_request(entity_series: pd.Series):
      entity_series[RuleActionReportColumn.api_request.value] = self.entity_request(
        entity_series=entity_series,
        api=api,
        context=context
      )
    location = action_report.loc[(action_report[RuleActionReportColumn.error.value].isna()) & (action_report[RuleActionReportColumn.log.value].notna()) & ((action_report[RuleActionReportColumn.override_preference.value]) | (action_report[RuleActionReportColumn.preference.value].apply(lambda p: p is not RuleActionPreference.prevent_adjustment)))]
    self.entity_apply(
      action_report=action_report,
      transformer=add_request,
      location=location
    )

  def entity_request(self, entity_series: pd.Series, api: any, context: any) -> Optional[any]:
    raise NotImplementedError()

  def commit_action_report_requests(self, api: any, action_report: pd.DataFrame, context: any):
    def commit_adjustment(entity_series: pd.Series):
        entity_series[RuleActionReportColumn.api_response.value] = self.mutate_entity(
          entity_series=entity_series,
          api=api,
          context=context
        )
    location = action_report.loc[(action_report[RuleActionReportColumn.error.value].isna()) & (action_report[RuleActionReportColumn.api_request.value].notna()) & (~action_report[RuleActionReportColumn.dry_run.value])]
    self.entity_apply(
      action_report=action_report,
      transformer=commit_adjustment,
      location=location
    )

  def mutate_entity(self, entity_series: pd.Series, api: any, context: any) -> Optional[any]:
    raise NotImplementedError()

  def finalize_action_report(self, action_report: pd.DataFrame, api: any, context: any) -> pd.DataFrame:
    action_report.loc[action_report[RuleActionReportColumn.error.value].notna(), RuleActionReportColumn.log.value] = None
    return action_report

class RuleMultiplierAction(RuleAction):
  @property
  def precision(self) -> Optional[int]:
    return None

  def entity_adjustment(self, entity_series: pd.Series, context: any) -> Optional[any]:
    unadjusted = entity_series[RuleActionReportColumn.unadjusted_state.value]
    adjusted = unadjusted * self.adjustmentValue
    if self.precision is not None:
      adjusted = round(adjusted, self.precision)
    if self.adjustmentValue > 1 and adjusted > self.adjustmentLimit:
      if unadjusted >= self.adjustmentLimit:
        return None
      adjusted = self.adjustmentLimit
    elif self.adjustmentValue < 1 and adjusted < self.adjustmentLimit:
      if unadjusted <= self.adjustmentLimit:
        return None
      adjusted = self.adjustmentLimit
    if adjusted == unadjusted:
      return None
    return adjusted

class RuleNoAction(RuleAction):
  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.no_action

  def entity_adjustment(self, entity_series: pd.Series, context: any) -> Optional[any]:
    return True

  def entity_request(self, entity_series: pd.Series, api: any, context: any) -> Optional[any]:
    return None

  def action_description(self, entity_series: pd.Series, context: any):
    return 'took no action'

class RulePauseAction(RuleAction):
  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.status

  @property
  def paused_value(self) -> str:
    raise NotImplementedError()

  @property
  def entity_type_description(self) -> str:
    raise NotImplementedError()

  def entity_adjustment(self, entity_series: str, context: any) -> any:
    return None if entity_series[RuleActionReportColumn.unadjusted_state.value] == self.paused_value else self.paused_value
  
  def action_description(self, entity_series: str, context: any) -> str:
    return f'paused {self.entity_type_description}'

