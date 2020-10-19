import pandas as pd

from regla import RuleAction, RuleActionResult, RuleActionLog, RuleActionTargetType, RuleActionPreference, RuleActionReportColumn, RuleReportColumn, RuleContext, RuleMultiplierAction, RuleNoAction, RuleActionAdjustmentType
from regla.errors import RuleActionMissingTargetError
from hazel import GoogleAdsAPI, GoogleAdsCampaignPauseMutator, GoogleAdsCampaignTargetCPAMutator, GoogleAdsCampaignBudgetMutator, GoogleAdsReporter
from datetime import datetime, timedelta
from pytz import timezone
from typing import Callable, List, Tuple, Dict, Optional
from bson import ObjectId
from .google_ads_context import GoogleAdsOption, GoogleAdsColumn, add_report_time

class GoogleAdsAction(RuleAction):
  @property
  def raw_entity_id_column(self) -> str:
    granularity = self.entity_granularity
    if granularity is RuleActionTargetType.campaign:
      return 'campaign_id'
    elif granularity is RuleActionTargetType.adgroup:
      return 'ad_group_id'
    else:
      raise ValueError('Unsupported entity granularity', granularity)

  @property
  def raw_entity_name_column(self) -> str:
    granularity = self.entity_granularity
    if granularity is RuleActionTargetType.campaign:
      return 'campaign_name'
    elif granularity is RuleActionTargetType.adgroup:
      return 'ad_group_name'
    else:
      raise ValueError('Unsupported entity granularity', granularity)

  @property
  def action_report_columns(self) -> List[str]:
    return [
      *super().action_report_columns,
      *[c.value for c in GoogleAdsColumn],
    ]

  @property 
  def preferences_title(self) -> str:
    return 'UAC best practices'

  def get_raw_action_report(self, entity_ids: List[str], api: GoogleAdsAPI, report: pd.DataFrame, context: any) -> pd.DataFrame:
    reporter = GoogleAdsReporter(api=api)
    action_report = reporter.get_safety_report(
      entity_granularity=self.entity_granularity.value,
      entity_ids=entity_ids
    )
    return action_report

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    action_report.rename(lambda s: s.replace('#', '_'), axis='columns', inplace=True)
    assert len(action_report.customer_time_zone.unique()) == 1
    time_zone = action_report.customer_time_zone.iloc[0]
    action_report.campaign_start_date = pd.to_datetime(action_report.campaign_start_date).dt.tz_localize(tz=time_zone, ambiguous='infer')
    action_report[RuleActionReportColumn.target_id.value] = action_report[self.raw_entity_id_column].astype(str)
    action_report[RuleActionReportColumn.target_name.value] = action_report[self.raw_entity_name_column]

  def supplement_action_report(self, action_report: pd.DataFrame, api: GoogleAdsAPI, context: any) -> pd.DataFrame:
    action_report = super().supplement_action_report(
      action_report=action_report,
      api=api,
      context=context
    )
    if context[RuleContext.rule_options.value][GoogleAdsOption.wait_days.value] <= 0:
      return action_report
    reporter = GoogleAdsReporter(api=context[RuleContext.channel.value].api)
    conversions_report_start_date = context[RuleContext.now.value] - timedelta(days=context[RuleContext.rule_options.value][GoogleAdsOption.wait_days.value])
    conversions_report = reporter.get_selected_conversions_report(
      start_date=conversions_report_start_date,
      end_date=context[RuleContext.now.value],
      entity_ids=list(action_report[RuleActionReportColumn.target_id.value]),
      entity_granularity=self.entity_granularity.value,
      time_granularity='hourly'
    )
    conversions_report.rename(lambda s: s.replace('#', '_'), axis='columns', inplace=True)
    action_report[GoogleAdsColumn.wait_metrics_since_time.value] = datetime(conversions_report_start_date.year, conversions_report_start_date.month, conversions_report_start_date.day, conversions_report_start_date.hour)
    action_report[GoogleAdsColumn.wait_conversions.value] = 0
    action_report[GoogleAdsColumn.wait_optimized_conversions.value] = 0

    add_report_time(report=conversions_report)
    def add_wait_metrics(entity_series: pd.Series):
      action_type_history = list(filter(lambda h: h['adjustmentType'] == self.adjustment_type.value, entity_series[RuleActionReportColumn.history.value]))
      if action_type_history:
        last_action = sorted(action_type_history, key=lambda h: h['historyCreationDate'])[-1]
        entity_series[GoogleAdsColumn.last_adjustment_time.value] = last_action['historyCreationDate']
        if entity_series[GoogleAdsColumn.last_adjustment_time.value] >= entity_series[GoogleAdsColumn.wait_metrics_since_time.value]:
          entity_series[GoogleAdsColumn.wait_metrics_since_time.value] = entity_series[GoogleAdsColumn.last_adjustment_time.value]
          entity_series[GoogleAdsColumn.last_adjustment_description.value] = f'adjustment by {"this rule" if str(last_action["ruleID"]) == context[RuleContext.rule.value]._id else "rule " + last_action["ruleDescription"] + " [" + str(last_action["ruleID"]) + "]"}'
      if conversions_report.empty:
        return
      location = conversions_report.loc[(conversions_report[self.raw_entity_id_column].astype(str) == entity_series[RuleActionReportColumn.target_id.value]) & (conversions_report.time >= entity_series[GoogleAdsColumn.wait_metrics_since_time.value])]
      entity_series[GoogleAdsColumn.wait_conversions.value] = location.total_conversions.sum()
      entity_series[GoogleAdsColumn.wait_optimized_conversions.value] = location.selected_conversions.sum()
    self.entity_apply(
      action_report=action_report,
      transformer=add_wait_metrics
    )
    return action_report
    
class GoogleAdsCampaignAction(GoogleAdsAction):
  @property
  def entity_granularity(self) -> RuleActionTargetType:
    return RuleActionTargetType.campaign

  def set_action_report_preferences(self, action_report: pd.DataFrame, api: any, context: any):
    location = action_report.loc[pd.isna(action_report[RuleActionReportColumn.error.value])]
    options = context[RuleContext.rule_options.value]
    # TODO: remove these testing overrides
    # options[GoogleAdsOption.wait_days.value] = 0.001
    # options[GoogleAdsOption.wait_conversions.value] = 10000
    # options[GoogleAdsOption.wait_optimized_conversions.value] = 3000
    wait_start_date = context[RuleContext.now.value] - timedelta(days=options[GoogleAdsOption.wait_days.value])
    wait_location = location.loc[location[GoogleAdsColumn.wait_metrics_since_time.value] > wait_start_date]
    wait_location = wait_location.loc[wait_location[GoogleAdsColumn.wait_conversions.value] < options[GoogleAdsColumn.wait_conversions.value]]
    wait_location = wait_location.loc[wait_location[GoogleAdsColumn.wait_optimized_conversions.value] < options[GoogleAdsColumn.wait_optimized_conversions.value]]
    self.add_preference(
      action_report=action_report,
      location=wait_location,
      preference=RuleActionPreference.prevent_adjustment,
      message_callback=lambda entity_series: f'Before adjusting campaign wait for one of {(context[RuleContext.now.value] - entity_series[GoogleAdsColumn.wait_metrics_since_time.value]).total_seconds() / 60 / 60 / 24 :0.2f} / {options[GoogleAdsOption.wait_days.value]} days or {int(entity_series[GoogleAdsColumn.wait_conversions.value])} / {options[GoogleAdsOption.wait_conversions.value]} converions or {int(entity_series[GoogleAdsColumn.wait_optimized_conversions.value])} / {options[GoogleAdsOption.wait_optimized_conversions.value]} optimized conversions{f" since {entity_series[GoogleAdsColumn.last_adjustment_description.value]} at {entity_series[GoogleAdsColumn.last_adjustment_time.value]} UTC" if entity_series[GoogleAdsColumn.last_adjustment_description.value] is not None else ""}'
    )

  def get_raw_action_report(self, entity_ids: List[str], api: GoogleAdsAPI, report: pd.DataFrame, context: any) -> pd.DataFrame:
    assert len(entity_ids) == 1
    assert entity_ids[0] == context['campaign_id']
    return super().get_raw_action_report(
      entity_ids=entity_ids,
      api=api,
      report=report,
      context=context
    )

class GoogleAdsPauseCampaignAction(GoogleAdsCampaignAction):
  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.status

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    super().map_action_report(
      action_report=action_report,
      context=context
    )
    action_report[RuleActionReportColumn.unadjusted_state.value] = action_report.campaign_status

  def entity_adjustment(self, entity_series: str, context: any) -> any:
    return None if entity_series[RuleActionReportColumn.unadjusted_state.value] == 'PAUSED' else 'PAUSED'

  def action_description(self, entity_series: str, context: any) -> str:
    return 'paused campaign'

  def entity_request(self, entity_series: pd.Series, api: GoogleAdsAPI, context: any) -> Optional[any]:
    return True

  def mutate_entity(self, entity_series: pd.Series, api: GoogleAdsAPI, context: any) -> Optional[any]:
    mutator = GoogleAdsCampaignPauseMutator(
      api=api,
      campaign_id=entity_series[RuleActionReportColumn.target_id.value]
    )
    raw_response = mutator.mutate()
    response = api.response_to_record(response=raw_response)
    return response

class GoogleAdsCampaignMultiplierAction(GoogleAdsCampaignAction, RuleMultiplierAction):
  @property
  def relative_adjustment_limit(self) -> float:
    return 0.2

  def supplement_action_report(self, action_report: pd.DataFrame, api: GoogleAdsAPI, context: any) -> pd.DataFrame:
    action_report = super().supplement_action_report(
      action_report=action_report,
      api=api,
      context=context
    )

    one_day_ago = datetime.utcnow() - timedelta(days=1)
    def add_prehistoric_state(entity_series: pd.Series):
      recent_history = sorted(filter(lambda h: h['historyCreationDate'] > one_day_ago and h['adjustmentType'] == self.adjustment_type.value, entity_series[RuleActionReportColumn.history.value]), key=lambda h: h['historyCreationDate'])
      entity_series[RuleActionReportColumn.prehistoric_state.value] = recent_history[0]['adjustmentFrom'] if recent_history else entity_series[RuleActionReportColumn.unadjusted_state.value]
    self.entity_apply(
      action_report=action_report,
      transformer=add_prehistoric_state
    )
    return action_report

  def set_action_report_preferences(self, action_report: pd.DataFrame, api: any, context: any):
    super().set_action_report_preferences(
      action_report=action_report,
      api=api,
      context=context
    )
    location = action_report.loc[pd.isna(action_report[RuleActionReportColumn.error.value])]
    def modify_adjustment(entity_series: pd.Series):
      if entity_series[RuleActionReportColumn.prehistoric_state.value] == 0:
        return
      current_relative_adjustment = entity_series[RuleActionReportColumn.unadjusted_state.value] / entity_series[RuleActionReportColumn.prehistoric_state.value]
      relative_adjustment = entity_series[RuleActionReportColumn.adjustment.value] / entity_series[RuleActionReportColumn.prehistoric_state.value]
      # if the current state is already adjusted at least to the limit, and we would adjust in the same direction
      if abs(current_relative_adjustment - 1) >= self.relative_adjustment_limit and (current_relative_adjustment - 1) * (relative_adjustment - 1) > 0:
        entity_series[RuleActionReportColumn.preference.value] = RuleActionPreference.prevent_adjustment
        entity_series[RuleActionReportColumn.preference_messages.value].append(f'Limit 1 day adjustment to {self.relative_adjustment_limit :0.0%}% of {entity_series[RuleActionReportColumn.prehistoric_state.value] :0.2f} (already {abs(current_relative_adjustment - 1) :0.0%} at {entity_series[RuleActionReportColumn.unadjusted_state.value] :0.2f})')
        return
      if abs(relative_adjustment - 1) > self.relative_adjustment_limit:
        entity_series[RuleActionReportColumn.preferred_adjustment.value] = (1 + self.relative_adjustment_limit if relative_adjustment > 1 else 1 - self.relative_adjustment_limit) * entity_series[RuleActionReportColumn.prehistoric_state.value]
        entity_series[RuleActionReportColumn.unpreferred_adjustment.value] = entity_series[RuleActionReportColumn.adjustment.value]
        if not entity_series[RuleActionReportColumn.override_preference.value]:
          entity_series[RuleActionReportColumn.adjustment.value] = entity_series[RuleActionReportColumn.preferred_adjustment.value]
    self.entity_apply(
      action_report=action_report,
      transformer=modify_adjustment,
      location=location.loc[(location[RuleActionReportColumn.prehistoric_state.value].notna()) & (location[RuleActionReportColumn.adjustment.value].notna())]
    )
    location = action_report.loc[pd.isna(action_report[RuleActionReportColumn.error.value])]
    self.add_preference(
      action_report=action_report,
      location=location.loc[location[RuleActionReportColumn.preferred_adjustment.value].notna()],
      preference=RuleActionPreference.modify_adjustment,
      message_callback=lambda e: f'Limit 1 day adjustment to {self.relative_adjustment_limit :0.0%} of {e[RuleActionReportColumn.prehistoric_state.value] :0.2f} ({e[RuleActionReportColumn.preferred_adjustment.value] :0.2f} instead of {e[RuleActionReportColumn.unpreferred_adjustment.value] :0.2f})'
    )

  def entity_adjustment(self, entity_series: pd.Series, context: any) -> Optional[any]:
    adjustment = super().entity_adjustment(
      entity_series=entity_series,
      context=context
    )
    if adjustment is None or int(adjustment * 1000000) == int(entity_series[RuleActionReportColumn.unadjusted_state.value] * 1000000):
      return None
    return adjustment

  def entity_request(self, entity_series: pd.Series, api: GoogleAdsAPI, context: any) -> Optional[any]:
    return True

class GoogleAdsTargetCPACampaignAction(GoogleAdsCampaignMultiplierAction):
  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.cpa_goal

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    super().map_action_report(
      action_report=action_report,
      context=context
    )
    assert 'campaign_bidding_strategy_type' in action_report.columns and action_report.campaign_bidding_strategy_type.unique() == ['TARGET_CPA'], 'Campaign bidding strategy type is not TARGET_CPA'
    action_report[RuleActionReportColumn.unadjusted_state.value] = action_report.campaign_target_cpa_target_cpa_micros.apply(lambda t: t / 1000000)
    assert action_report.campaign_target_cpa_target_cpa_micros.min() >= 0

  def action_description(self, entity_series: pd.Series, context: any):
    if int(entity_series[RuleActionReportColumn.adjustment.value] * 1000000) == int(entity_series[RuleActionReportColumn.unadjusted_state.value] * 1000000):
      return None
    return f'adjusted campaign target CPA from {entity_series[RuleActionReportColumn.unadjusted_state.value] :0.2f} to {entity_series[RuleActionReportColumn.adjustment.value] :0.2f}'

  def mutate_entity(self, entity_series: pd.Series, api: GoogleAdsAPI, context: any) -> Optional[any]:
    mutator = GoogleAdsCampaignTargetCPAMutator(
      api=api,
      campaign_id=entity_series[RuleActionReportColumn.target_id.value],
      target_cpa_micros=int(entity_series[RuleActionReportColumn.adjustment.value] * 1000000)
    )
    raw_response = mutator.mutate()
    response = api.response_to_record(response=raw_response)
    return response

class GoogleAdsCampaignBudgetAction(GoogleAdsCampaignMultiplierAction):
  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.budget

  @property
  def precision(self) -> Optional[int]:
    return 2

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    super().map_action_report(
      action_report=action_report,
      context=context
    )
    assert 'campaign_budget_type' in action_report.columns and action_report.campaign_budget_type.unique() == ['STANDARD'], 'Campaign budget type is not STANDARD'
    assert 'campaign_budget_status' in action_report.columns and action_report.campaign_budget_status.unique() == ['ENABLED'], 'Campaign budget status is not ENABLED'
    assert 'campaign_budget_period' in action_report.columns and action_report.campaign_budget_period.unique() == ['DAILY'], 'Campaign budget period is not DAILY'
    action_report[RuleActionReportColumn.unadjusted_state.value] = action_report.campaign_budget_amount_micros.apply(lambda t: t / 1000000)
    assert action_report.campaign_budget_amount_micros.min() >= 0

  def action_description(self, entity_series: pd.Series, context: any):
    if int(entity_series[RuleActionReportColumn.adjustment.value] * 1000000) == int(entity_series[RuleActionReportColumn.unadjusted_state.value] * 1000000):
      return None
    return f'adjusted campaign budget from {entity_series[RuleActionReportColumn.unadjusted_state.value] :0.2f} to {entity_series[RuleActionReportColumn.adjustment.value] :0.2f}'

  def mutate_entity(self, entity_series: pd.Series, api: GoogleAdsAPI, context: any) -> Optional[any]:
    mutator = GoogleAdsCampaignBudgetMutator(
      api=api,
      campaign_id=entity_series[RuleActionReportColumn.target_id.value],
      budget_micros=int(entity_series[RuleActionReportColumn.adjustment.value] * 1000000),
      budget_name=f'Rule [{context[RuleContext.rule.value]._id}] budget change from {entity_series[RuleActionReportColumn.unadjusted_state.value]} at {context[RuleContext.now.value]}'
    )
    raw_response = mutator.mutate()
    response = api.response_to_record(response=raw_response)
    return response

class GoogleAdsNoAction(GoogleAdsCampaignAction, RuleNoAction):
  def entity_adjustment(self, entity_series: pd.Series, context: any) -> Optional[any]:
    return None
