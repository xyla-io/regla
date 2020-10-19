import os
import pandas as pd

from .apple_search_ads_reporter import SearchAdsReporter
from .actions import SearchAdsBidAction, SearchAdsPauseKeywordAction, SearchAdsCPAGoalAction, SearchAdsNoAction

from regla import Channel, ChannelEntity, RuleAction, RuleActionType, RuleReportType, RuleReporter, RuleReportGranularity, Rule, RuleContext
from heathcliff import AppleSearchAdsCertificate
from heathcliff.mutating import SearchAdsAccount, SearchAds
from bson import ObjectId
from datetime import datetime
from typing import Optional, List, Dict

class AppleSearchAdsChannel(Channel[SearchAds, any]):
  certificate: Optional[AppleSearchAdsCertificate]=None
  parent_environ: Optional[Dict[str, any]]=None

  @property
  def identifier(self) -> str:
    return 'apple_search_ads'

  @property
  def title(self) -> str:
    return 'Apple'

  def connect(self, credentials: Dict[str, any]):
    self.certificate = AppleSearchAdsCertificate(certificate=credentials)
    self.certificate.connect()
    self.parent_environ = dict(os.environ)
    certs = {
      'SEARCH-ADS-PEM': self.certificate.pem_path,
      'SEARCH-ADS-KEY': self.certificate.key_path,
    }
    os.environ.update(certs)
    self.api = SearchAds(org_name=self.certificate.org_name)
  
  def disconnect(self):
    super().disconnect()
    os.environ.clear()
    os.environ.update(self.parent_environ)
    self.parent_environ = None
    self.certificate.disconnect()
    self.certificate = None

  def rule_context(self, options: Dict[str, any]={}) -> any:
    rule: Rule = options[RuleContext.rule.value]
    self.api.org_name = ''
    self.api.org_id = rule.orgID
    campaigns = self.api.get_campaigns(includeAdGroups=False, includeKeywords=False)
    matching_campaigns = [c for c in campaigns if int(c._id) == int(rule.campaignID)]
    if not matching_campaigns:
      return None

    campaign = matching_campaigns[0]
    if rule.adgroupID is None:
      campaign.ad_groups = self.api.get_adgroups(campaignID=campaign._id)
    else:
      campaign.ad_groups = self.api.get_adgroups(campaignID=campaign._id, includeKeywords=False)
      for ad_group in campaign.ad_groups:
        if int(ad_group._id) == int(rule.adgroupID):
          ad_group.keywords = self.api.get_keywords(campaignID=campaign._id, adGroupID=ad_group._id)
    return campaign

  def report_type(self, action_type: RuleActionType) -> RuleReportType:
    if action_type is RuleActionType.increaseBid or action_type is RuleActionType.decreaseBid:
      return RuleReportType.keyword
    elif action_type is RuleActionType.increaseCPAGoal or action_type is RuleActionType.decreaseCPAGoal:
      return RuleReportType.adGroup
    elif action_type is RuleActionType.pauseKeyword:
      return RuleReportType.keyword
    elif action_type is RuleActionType.noAction:
      return RuleReportType.keyword
    else:
      raise ValueError('Unsupported search ads action', action_type)

  def rule_reporter(self, report_type: Optional[RuleReportType]=None, ad_group_id: Optional[str]=None, rule_id: Optional[ObjectId]=None, data_check_range: Optional[int]=None, raw_report: Optional[pd.DataFrame]=None, report:Optional[pd.DataFrame]=None) -> RuleReporter:
    return SearchAdsReporter(
      reportType=report_type,
      adGroupID=ad_group_id,
      ruleID=rule_id,
      dataCheckRange=data_check_range,
      rawReport=raw_report,
      report=report
    )

  def rule_action(self, action_type: RuleActionType, adjustment_value: Optional[float]=None, adjustment_limit: Optional[float]=None) -> RuleAction:
    if action_type is RuleActionType.increaseBid:
      adjustment = 1.0 + adjustment_value / 100.0
      return SearchAdsBidAction(type=action_type, adjustmentValue=adjustment, adjustmentLimit=adjustment_limit)
    elif action_type is RuleActionType.decreaseBid:
      adjustment = 1.0 - adjustment_value / 100.0
      return SearchAdsBidAction(type=action_type, adjustmentValue=adjustment, adjustmentLimit=adjustment_limit)
    elif action_type is RuleActionType.increaseCPAGoal:
      adjustment = 1.0 + adjustment_value / 100.0
      return SearchAdsCPAGoalAction(type=action_type, adjustmentValue=adjustment, adjustmentLimit=adjustment_limit)
    elif action_type is RuleActionType.decreaseCPAGoal:
      adjustment = 1.0 - adjustment_value / 100.0
      return SearchAdsCPAGoalAction(type=action_type, adjustmentValue=adjustment, adjustmentLimit=adjustment_limit)
    elif action_type is RuleActionType.pauseKeyword:
      return SearchAdsPauseKeywordAction(type=action_type)
    elif action_type is RuleActionType.noAction:
      return SearchAdsNoAction(type=action_type)
    else:
      raise ValueError('Unsupported action type', action_type)
  
  def get_entities(self, entity_type: ChannelEntity, parent_ids: Dict[ChannelEntity, str]={}) -> List[Dict[str, any]]:
    if entity_type is ChannelEntity.org:
      account = SearchAdsAccount()
      orgs = []
      for api in account.apis:
        orgs.append({
          'id' : int(api.org_id),
          'name' : api.org_name,
        })
      return orgs
    elif entity_type is ChannelEntity.campaign:
      self.api.org_id = int(parent_ids[ChannelEntity.org])
      campaigns = []
      for campaign in self.api.get_campaigns(includeAdGroups=False, includeKeywords=False):
        campaigns.append({
          'org_id': int(self.api.org_id),
          'id' : int(campaign._id),
          'name' : campaign.name,
        })
      return campaigns
    elif entity_type is ChannelEntity.ad_group:
      self.api.org_id = int(parent_ids[ChannelEntity.org])
      campaign_id = parent_ids[ChannelEntity.campaign]
      ad_groups = []
      for ad_group in self.api.get_adgroups(campaignID=campaign_id, includeKeywords=False):
        ad_groups.append({
          'org_id': int(self.api.org_id),
          'campaign_id': int(campaign_id),
          'id' : int(ad_group._id),
          'name' : ad_group.name
        })
      return ad_groups
    else:
      raise ValueError('Unsupported entity type', entity_type)

  def granularity_is_compatible(self, granularity: RuleReportGranularity, report_type: RuleReportType, start_date: datetime, end_date: datetime):
    now = datetime.utcnow()
    interval = end_date - start_date
    age = now - start_date
    monthInterval = end_date.month - start_date.month + (end_date.year - start_date.year) * 12
    monthAge = now.month - start_date.month + (now.year - start_date.year) * 12
    if granularity is RuleReportGranularity.hourly:
        if report_type is RuleReportType.searchTerm: return False
        if interval.days > 7: return False
        if age.days > 30: return False
        return True
    elif granularity is RuleReportGranularity.daily:
        if interval.days > 90: return False
        if age.days > 730: return False
        return True
    elif granularity is RuleReportGranularity.weekly:
        if interval.days <= 14: return False
        if interval.days > 365: return False
        if monthAge > 24: return False
        return True
    elif granularity is RuleReportGranularity.monthly:
        if monthInterval <= 3: return False
        if monthAge > 24: return False
        return True
    else:
        raise ValueError('Unsupported report granularity', granularity)