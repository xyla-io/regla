import pandas as pd

from bson import ObjectId
from regla import Channel, ChannelEntity, RuleAction, RuleActionType, RuleReportType, RuleReporter, RuleContext, RuleOption, Rule
from hazel import GoogleAdsAPI
from typing import Optional, List, Dict
from .google_ads_context import GoogleAdsContext, GoogleAdsOption
from .google_ads_actions import GoogleAdsTargetCPACampaignAction, GoogleAdsCampaignBudgetAction, GoogleAdsPauseCampaignAction, GoogleAdsNoAction
from .google_ads_reporter import GoogleAdsReporter

class GoogleAdsChannel(Channel[GoogleAdsAPI, Dict[str, any]]):
  @property
  def identifier(self) -> str:
    return 'google_ads'

  @property
  def title(self) -> str:
    return 'Google'

  def connect(self, credentials: Dict[str, any]):
    self.api = GoogleAdsAPI(**credentials)

  def rule_context(self, options: Dict[str, any]={}) -> Dict[str, any]:
    rule: Rule = options[RuleContext.rule.value]
    return {
      GoogleAdsContext.campaign_id.value: str(int(rule.campaignID)),
      **{c.value: options[c.value] for c in [
        RuleContext.channel,
        RuleContext.now,
        RuleContext.rule,
        RuleContext.rule_collection,
        RuleContext.history_collection,
      ]},
      RuleContext.rule_options.value: {
        **RuleOption.get_defaults(),
        RuleOption.dynamic_window.value: False,
        **GoogleAdsOption.get_defaults(),
        **rule.options,
      },
    }

  def report_type(self, action_type: RuleActionType) -> RuleReportType:
    if action_type is RuleActionType.increaseCPAGoalCampaign:
      return RuleReportType.campaign
    elif action_type is RuleActionType.decreaseCPAGoalCampaign:
      return RuleReportType.campaign
    elif action_type is RuleActionType.pauseCampaign:
      return RuleReportType.campaign
    elif action_type is RuleActionType.noAction:
      return RuleReportType.campaign
    elif action_type is RuleActionType.increase_camapign_budget:
      return RuleReportType.campaign
    elif action_type is RuleActionType.decrease_campaign_budget:
      return RuleReportType.campaign
    else:
      raise ValueError('Unsupported action type', action_type)

  def rule_reporter(self, report_type: Optional[RuleReportType]=None, ad_group_id: Optional[str]=None, rule_id: Optional[ObjectId]=None, data_check_range: Optional[int]=None, raw_report: Optional[pd.DataFrame]=None, report:Optional[pd.DataFrame]=None) -> RuleReporter:
    return GoogleAdsReporter(
      reportType=report_type,
      adGroupID=ad_group_id,
      ruleID=rule_id,
      dataCheckRange=data_check_range,
      rawReport=raw_report,
      report=report
    )

  def rule_action(self, action_type: RuleActionType, adjustment_value: Optional[float]=None, adjustment_limit: Optional[float]=None) -> RuleAction:
    if action_type is RuleActionType.increaseCPAGoalCampaign:
      adjustment = 1.0 + adjustment_value / 100.0
      return GoogleAdsTargetCPACampaignAction(
        type=action_type,
        adjustmentValue=adjustment,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.decreaseCPAGoalCampaign:
      adjustment = 1.0 - adjustment_value / 100.0
      return GoogleAdsTargetCPACampaignAction(
        type=action_type,
        adjustmentValue=adjustment,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.increase_camapign_budget:
      adjustment = 1.0 + adjustment_value / 100.0
      return GoogleAdsCampaignBudgetAction(
        type=action_type,
        adjustmentValue=adjustment,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.decrease_campaign_budget:
      adjustment = 1.0 - adjustment_value / 100.0
      return GoogleAdsCampaignBudgetAction(
        type=action_type,
        adjustmentValue=adjustment,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.pauseCampaign:
      return GoogleAdsPauseCampaignAction(
        type=action_type,
        adjustmentValue=adjustment_value,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.noAction:
      return GoogleAdsNoAction(
        type=action_type,
        adjustmentValue=adjustment_value,
        adjustmentLimit=adjustment_limit
      )
    else:
      raise ValueError('Unsupported action type', action_type)

  def get_entities(self, entity_type: ChannelEntity, parent_ids: Dict[ChannelEntity, str]={}) -> List[Dict[str, any]]:
    if entity_type is ChannelEntity.org:
      org_ids = self.api.get_customers()
      org_data = [
        {
          'id': i,
          **self.api.get_customer_metadata(customer_id=i),
        }
        for i in org_ids
      ]
      return [
        {
          'id': int(d['id']),
          'name': d['customer_descriptive_name'],
        }
        for d in org_data if not d['customer_manager']
      ]
    elif entity_type is ChannelEntity.campaign:
      org_id = parent_ids[ChannelEntity.org]
      campaign_data = self.api.get_campaigns(customer_id=org_id)
      return [
        {
          'org_id': int(org_id),
          'id': int(d['campaign_id']),
          'name': d['campaign_name'],
        }
        for d in campaign_data
      ]
    elif entity_type is ChannelEntity.ad_group:
      org_id = parent_ids[ChannelEntity.org]
      campaign_id = parent_ids[ChannelEntity.campaign]
      ad_group_data = self.api.get_ad_groups(
        customer_id=org_id,
        campaign_id=campaign_id
      )
      return [
        {
          'org_id': int(org_id),
          'campaign_id': int(campaign_id),
          'id': int(d['ad_group_id']),
          'name': d['ad_group_name'],
        }
        for d in ad_group_data
      ]
    else:
      raise ValueError('Unsupported entity type', entity_type)