import pandas as pd

from typing import Optional, List
from datetime import datetime
from regla import RuleContext, RuleAction, RulePauseAction, RuleActionTargetType, RuleNoAction, RuleReportGranularity, RuleActionReportColumn, RuleMultiplierAction, RuleActionAdjustmentType, RuleActionPreference
from azrael import SnapchatAPI, SnapchatCampaignPauseMutator, SnapchatCampaignBudgetMutator
from .snapchat_reporters import SnapchatRawCampaignReporter

class SnapchatAction(RuleAction):
  @property
  def raw_columns(self) -> List[str]:
    return [
      'name',
    ]

  def entity_target_id(self, entity_id: str) -> any:
    return entity_id

  @property 
  def preferences_title(self) -> str:
    return 'Snapchat requirements'

class SnapchatCampaignAction(SnapchatAction):
  @property
  def entity_granularity(self) -> RuleActionTargetType:
    return RuleActionTargetType.campaign

  def get_raw_action_report(self, entity_ids: List[str], api: SnapchatAPI, report: pd.DataFrame, context: any) -> pd.DataFrame:
    reporter = SnapchatRawCampaignReporter(
      columns=self.raw_columns,
    )
    report = reporter.run(
      start_date=None,
      end_date=None,
      granularity=RuleReportGranularity.hourly.value,
      api=api,
      context=context
    )['raw_report']
    return report

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    action_report[RuleActionReportColumn.target_id.value] = action_report['campaign_id']
    action_report[RuleActionReportColumn.target_name.value] = action_report['name']
    super().map_action_report(
      action_report=action_report,
      context=context
    )

  def entity_request(self, entity_series: pd.Series, api: SnapchatAPI, context: any) -> Optional[any]:
    return True

class SnapchatCampaignNoAction(SnapchatCampaignAction, RuleNoAction):
  def entity_request(self, entity_series: pd.Series, api: SnapchatAPI, context: any) -> Optional[any]:
    return None

class SnapchatPauseCampaignAction(SnapchatCampaignAction, RulePauseAction):
  @property
  def raw_columns(self) -> List[str]:
    return [
      *super().raw_columns,
      'status',
    ]

  @property
  def paused_value(self) -> str:
    return 'PAUSED'

  @property
  def entity_type_description(self) -> str:
    return 'campaign'

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    action_report[RuleActionReportColumn.unadjusted_state.value] = action_report['status']
    super().map_action_report(
      action_report=action_report,
      context=context
    )

  def mutate_entity(self, entity_series: pd.Series, api: SnapchatAPI, context: any) -> Optional[any]:
    mutator = SnapchatCampaignPauseMutator(
      api=api,
      campaign_id=entity_series[RuleActionReportColumn.target_id.value]
    )
    raw_response = mutator.mutate()
    response = raw_response
    return response

class SnapchatCampaignBudgetAction(SnapchatCampaignAction, RuleMultiplierAction):
  @property
  def raw_columns(self) -> List[str]:
    return [
      *super().raw_columns,
      'daily_budget_micro',
    ]

  @property
  def adjustment_type(self) -> RuleActionAdjustmentType:
    return RuleActionAdjustmentType.budget

  @property
  def precision(self) -> Optional[int]:
    return 2

  def map_action_report(self, action_report: pd.DataFrame, context: any):
    action_report[RuleActionReportColumn.unadjusted_state.value] = action_report['daily_budget_micro'] / 1000000
    super().map_action_report(
      action_report=action_report,
      context=context
    )

  def action_description(self, entity_series: pd.Series, context: any):
    return f'adjusted campaign budget from {entity_series[RuleActionReportColumn.unadjusted_state.value] :0.2f} to {entity_series[RuleActionReportColumn.adjustment.value] :0.2f}'

  def set_action_report_preferences(self, action_report: pd.DataFrame, api: any, context: any):
    location = action_report.loc[pd.isna(action_report[RuleActionReportColumn.error.value])]
    minimum_budget = 20
    def modify_adjustment(entity_series: pd.Series):
      adjustment = entity_series[RuleActionReportColumn.adjustment.value]
      unadjusted = entity_series[RuleActionReportColumn.unadjusted_state.value]
      if adjustment >= minimum_budget:
        return
      if unadjusted <= minimum_budget:
        entity_series[RuleActionReportColumn.preference.value] = RuleActionPreference.prevent_adjustment
        entity_series[RuleActionReportColumn.preference_messages.value].append(f'Snapchat does not support campaign budgets of less than {minimum_budget :0.2f} (already at {unadjusted :0.2f})')
      else:
        entity_series[RuleActionReportColumn.preferred_adjustment.value] = minimum_budget
        entity_series[RuleActionReportColumn.unpreferred_adjustment.value] = entity_series[RuleActionReportColumn.adjustment.value]
        if not entity_series[RuleActionReportColumn.override_preference.value]:
          entity_series[RuleActionReportColumn.adjustment.value] = entity_series[RuleActionReportColumn.preferred_adjustment.value]
    self.entity_apply(
      action_report=action_report,
      transformer=modify_adjustment,
      location=location.loc[location[RuleActionReportColumn.adjustment.value].notna()]
    )
    location = action_report.loc[pd.isna(action_report[RuleActionReportColumn.error.value])]
    self.add_preference(
      action_report=action_report,
      location=location.loc[location[RuleActionReportColumn.preferred_adjustment.value].notna()],
      preference=RuleActionPreference.modify_adjustment,
      message_callback=lambda e: f'Reduce campaign budget to not less than {minimum_budget :0.2f} (instead of {e[RuleActionReportColumn.unpreferred_adjustment.value] :0.2f})'
    )

  def mutate_entity(self, entity_series: pd.Series, api: SnapchatAPI, context: any) -> Optional[any]:
    mutator = SnapchatCampaignBudgetMutator(
      api=api,
      campaign_id=entity_series[RuleActionReportColumn.target_id.value],
      daily_budget_micro=int(entity_series[RuleActionReportColumn.adjustment.value] * 1000000)
    )
    raw_response = mutator.mutate()
    response = raw_response
    return response

