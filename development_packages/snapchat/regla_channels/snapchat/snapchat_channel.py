import pandas as pd

from bson import ObjectId
from regla import Channel, Rule, RuleContext, RuleActionType, RuleReportType, RuleReporter, ChannelEntity, RuleAction, RuleOption
from azrael import SnapchatAPI
from typing import Optional, Dict, List
from .snapchat_context import SnapchatContext
from .snapchat_reporters import SnapchatReporter
from .snapchat_actions import SnapchatPauseCampaignAction, SnapchatCampaignNoAction, SnapchatCampaignBudgetAction

class SnapchatChannel(Channel[SnapchatAPI, Dict[str, any]]):
  @property
  def identifier(self) -> str:
    return 'snapchat'
  
  @property
  def title(self) -> str:
    return 'Snapchat'

  def connect(self, credentials: Dict[str, any]):
    self.api = SnapchatAPI(**credentials)

  def rule_context(self, options: Dict[str, any]={}) -> Dict[str, any]:
    rule: Rule = options[RuleContext.rule.value]
    return {
      **options,
      RuleContext.rule_options.value: {
        **RuleOption.get_defaults(),
        RuleOption.dynamic_window.value: False,
        **rule.options,
      },
      SnapchatContext.campaign_id.value: rule.campaignID,
    }

  def report_type(self, action_type: RuleActionType) -> RuleReportType:
    if action_type is RuleActionType.pauseCampaign:
      return RuleReportType.campaign
    elif action_type is RuleActionType.increase_camapign_budget:
      return RuleReportType.campaign
    elif action_type is RuleActionType.decrease_campaign_budget:
      return RuleReportType.campaign
    elif action_type is RuleActionType.noAction:
      return RuleReportType.campaign
    else:
      raise ValueError('Unsupported action type', action_type)

  def rule_reporter(self, report_type: Optional[RuleReportType]=None, ad_group_id: Optional[str]=None, rule_id: Optional[ObjectId]=None, data_check_range: Optional[int]=None, raw_report: Optional[pd.DataFrame]=None, report:Optional[pd.DataFrame]=None) -> RuleReporter:
    return SnapchatReporter(
      reportType=report_type,
      adGroupID=ad_group_id,
      ruleID=rule_id,
      dataCheckRange=data_check_range,
      rawReport=raw_report,
      report=report
    )

  def rule_action(self, action_type: RuleActionType, adjustment_value: Optional[float]=None, adjustment_limit: Optional[float]=None) -> RuleAction:
    if action_type is RuleActionType.noAction:
      return SnapchatCampaignNoAction(
        type=action_type,
        adjustmentValue=adjustment_value,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.pauseCampaign:
      return SnapchatPauseCampaignAction(
        type=action_type,
        adjustmentValue=adjustment_value,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.increase_camapign_budget:
      return SnapchatCampaignBudgetAction(
        type=action_type,
        adjustmentValue=1 + adjustment_value / 100,
        adjustmentLimit=adjustment_limit
      )
    elif action_type is RuleActionType.decrease_campaign_budget:
      return SnapchatCampaignBudgetAction(
        type=action_type,
        adjustmentValue=1 - adjustment_value / 100,
        adjustmentLimit=adjustment_limit
      )
    else:
      raise ValueError('Unsupported action type', action_type)

  def get_entities(self, entity_type: ChannelEntity, parent_ids: Dict[ChannelEntity, str]={}) -> List[Dict[str, any]]:
    if entity_type is ChannelEntity.org:
      ad_account = self.api.get_ad_account()
      return [
        {
          'id': ad_account['id'],
          'name': ad_account['name']
        }
      ]
    elif entity_type is ChannelEntity.campaign:
      ad_account_id = parent_ids[ChannelEntity.org]
      campaign_data = self.api.get_campaigns(ad_account_id=ad_account_id)
      return [
        {
          'org_id': ad_account_id,
          'id': d['id'],
          'name': d['name']
        }
        for d in campaign_data
      ]
    elif entity_type is ChannelEntity.ad_group:
      ad_account_id = parent_ids[ChannelEntity.org]
      campaign_id = parent_ids[ChannelEntity.campaign]
      ad_squad_data = self.api.get_ad_squads(ad_account_id=ad_account_id)
      return [
        {
          'org_id': ad_account_id,
          'campaign_id': campaign_id,
          'id': d['id'],
          'name': d['name']
        }
        for d in ad_squad_data
        if d['campaign_id'] == campaign_id
      ]
