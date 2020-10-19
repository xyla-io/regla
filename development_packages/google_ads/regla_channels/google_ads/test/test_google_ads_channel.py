import pytest
import pandas as pd

from typing import Dict, List, Optional
from unittest import mock
from ..google_ads_channel import GoogleAdsChannel 
from ..google_ads_reporter import GoogleAdsReporter
from ..google_ads_actions import GoogleAdsPauseCampaignAction
from hazel import GoogleAdsReporter as HazelReporter, GoogleAdsAPI as HazelAPI
from regla import ChannelEntity, RuleActionType, RuleReportType, RuleContext, Rule

@pytest.fixture
def channel() -> GoogleAdsChannel:
  yield GoogleAdsChannel()

@pytest.fixture
def credentials() -> Dict[str, any]:
  yield {
    'developer_token': '',
    'client_id': '',
    'client_secret': '',
    'refresh_token': '',
    'login_customer_id': '',
    'customer_id': '',
  }

@pytest.fixture(autouse=True)
def hazel_api(credentials: Dict[str, any]) -> HazelAPI:
  customer_metadata = {
    'customer_manager': False,
    'customer_descriptive_name': '',
  }
  with mock.patch.object(HazelAPI, 'get_customers', return_value=['4', '8', '0']), \
    mock.patch.object(HazelAPI, 'get_campaigns', return_value=[{ 'campaign_id': '', 'campaign_name': '' }]), \
    mock.patch.object(HazelAPI, 'get_ad_groups', return_value=[{ 'ad_group_id': '', 'ad_group_name': '' }]), \
    mock.patch.object(HazelAPI, 'get_customer_metadata', return_value=customer_metadata), \
    mock.patch.object(HazelAPI, '__init__', return_value=None):
    yield HazelAPI(**credentials)

def test_identifier(channel: GoogleAdsChannel):
  assert channel.identifier == 'google_ads'

@pytest.mark.incremental
class TestRuleContext:
  @pytest.fixture(autouse=True)
  def options(self) -> Dict[str, any]:
    rule = mock.Mock()
    rule.campaignID = '10'
    rule.userID = '6'
    yield {
      'campaign_id': '10',
      'rule': rule,
      'rule_collection': '',
      'history_collection': '',
    }

  def test_execute(self, channel: GoogleAdsChannel, options: Dict[str, any]):
    channel.rule_context(options)

  def test_shape(self, channel: GoogleAdsChannel, options: Dict[str, any]):
    assert channel.rule_context(options)['campaign_id']
  
  def test_content(self, channel: GoogleAdsChannel, options: Dict[str, any]):
    assert channel.rule_context(options)['campaign_id'] == '10' and channel.rule_context(options)['user_id'] == '6'

class TestReportType:
  def test_pause_campaign(self, channel: GoogleAdsChannel):
    assert channel.report_type(RuleActionType.pauseCampaign) == RuleReportType.campaign

  def test_increase_bid(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.increaseBid)

  def test_decrease_bid(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.decreaseBid)

  def test_increase_cpa_goal(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.increaseCPAGoal)
  
  def test_decrease_cpa_goal(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.decreaseCPAGoal)

  def test_pause_keyword(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.pauseKeyword)

  def test_no_action(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.noAction)

def test_rule_reporter(channel: GoogleAdsChannel):
  reporter = channel.rule_reporter(report_type=RuleReportType.campaign) 
  assert isinstance(reporter, GoogleAdsReporter)

class TestRuleAction:
  def test_pause_campaign(self, channel: GoogleAdsChannel):
    assert isinstance(
      channel.rule_action(action_type=RuleActionType.pauseCampaign),
      GoogleAdsPauseCampaignAction
    )
  
  def test_increase_bid(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.increaseBid)

  def test_decrease_bid(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.decreaseBid)

  def test_increase_cpa_goal(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.increaseCPAGoal)

  def test_decrease_cpa_goal(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.decreaseCPAGoal)

  def test_pause_keyword(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.pauseKeyword)

  def test_no_action(self, channel: GoogleAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.noAction)

@pytest.mark.incremental
class TestGetEntitiesOrg:
  @pytest.fixture(autouse=True)
  def setup(self, channel: GoogleAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.orgs = channel.get_entities(entity_type=ChannelEntity.org)
    yield

  def test_execute(self):
    assert self.orgs
  
  def test_shape(self, hazel_api: HazelAPI):
    assert len(self.orgs) == len(hazel_api.get_customers())
    assert set(self.orgs[0].keys()) == { 'id', 'name' }

@pytest.mark.incremental
class TestGetEntitiesCampaign:
  @pytest.fixture(autouse=True)
  def setup(self, channel: GoogleAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.campaigns = channel.get_entities(entity_type=ChannelEntity.campaign, parent_ids={ChannelEntity.org: '1'})
    yield

  def test_execute(self):
    assert self.campaigns
  
  def test_shape(self, hazel_api: HazelAPI):
    assert set(self.campaigns[0].keys()) == { 'org_id', 'id', 'name' }
    assert len(self.campaigns) == len(hazel_api.get_campaigns())

@pytest.mark.incremental
class TestGetEntitiesAdGroup:
  @pytest.fixture(autouse=True)
  def setup(self, channel: GoogleAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.ad_groups = channel.get_entities(entity_type=ChannelEntity.ad_group, parent_ids={
        ChannelEntity.org: '1',
        ChannelEntity.campaign: '2',
      })
    yield

  def test_execute(self):
    assert self.ad_groups
  
  def test_shape(self, hazel_api: HazelAPI):
    assert set(self.ad_groups[0].keys()) == { 'org_id', 'campaign_id', 'id', 'name' }
    assert len(self.ad_groups) == len(hazel_api.get_ad_groups())