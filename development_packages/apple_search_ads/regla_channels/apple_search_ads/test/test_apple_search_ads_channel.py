import pandas as pd
import pytest

from datetime import datetime, timedelta
from typing import Dict, List
from unittest import mock
from ..apple_search_ads_channel import AppleSearchAdsChannel
from ..apple_search_ads_reporter import SearchAdsReporter
from ..actions import SearchAdsBidAction, SearchAdsCPAGoalAction, SearchAdsPauseKeywordAction, SearchAdsNoAction
from regla import ChannelEntity, RuleActionType, RuleReportType, RuleReportGranularity, Rule
from heathcliff.models import Campaign, AdGroup 
from heathcliff.mutating import SearchAds, SearchAdsAccount

@pytest.fixture
def channel() -> AppleSearchAdsChannel:
  yield AppleSearchAdsChannel()

@pytest.fixture
def reporter() -> SearchAdsReporter:
  pass

@pytest.fixture
def credentials() -> Dict[str, any]:
  yield {
    'org_name': '',
    'pem': '',
    'key': '',
  }

@pytest.fixture(autouse=True)
def search_ads_api(credentials: Dict[str, any]) -> SearchAds:
  org_name = 'y'
  with mock.patch.object(SearchAds, 'get_campaigns', return_value=[Campaign(id=10, name='b'), Campaign(id=11, name='y')]),\
    mock.patch.object(SearchAds, 'get_adgroups', return_value=[AdGroup(id=20, name='b'), AdGroup(id=21, name='y'), AdGroup(id=22, name='v')]), \
    mock.patch.object(SearchAds, 'get_keywords', return_value=[type('MockKeyword', (), {'id': 30})]):
    yield SearchAds(org_name=org_name)

@pytest.fixture(autouse=True)
def search_ads_account(credentials: Dict[str, any]) -> SearchAdsAccount:
  with mock.patch.object(SearchAdsAccount, 'get_orgs', return_value={1: 'y', 2: 'b'}):
    yield SearchAdsAccount() 

def test_identifier(channel: AppleSearchAdsChannel):
  assert channel.identifier == 'apple_search_ads'

class TestRuleContext:
  def test_rule_context_campaign_mismatch(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      mock_rule = type('MockRule', (), {
        'campaignID': 100, # Not available in the mocked SearchAds.get_campaigns()
        'adgroupID': None,
        'orgID': None
      })()
      assert channel.rule_context(options={'rule': mock_rule}) is None

  def test_rule_context_campaign(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any], search_ads_api: SearchAds):
    with channel.connected(credentials=credentials):
      mock_rule = type('MockRule', (), {
        'campaignID': 10, # Available in the mocked SearchAds.get_campaigns()
        'adgroupID': None,
        'orgID': None
      })()
      context = channel.rule_context(options={'rule': mock_rule})
      assert int(context._id) == mock_rule.campaignID
      assert len(context.ad_groups) == len(search_ads_api.get_adgroups())

  def test_rule_context_campaign_with_adgroup(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any], search_ads_api: SearchAds):
    with channel.connected(credentials=credentials):
      mock_rule = type('MockRule', (), {
        'campaignID': 10,
        'adgroupID': 20,
        'orgID': None
      })()
      context = channel.rule_context(options={'rule': mock_rule})
      assert len(context.ad_groups) == len(search_ads_api.get_adgroups())
      target_adgroup = [a for a in context.ad_groups if a._id == '20'][0]
      assert len(target_adgroup.keywords) == len(search_ads_api.get_keywords())
      for adgroup in context.ad_groups:
        if adgroup._id != '20':
          assert len(adgroup.keywords) == 0

class TestReportType:
  def test_increase_bid(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.increaseBid) == RuleReportType.keyword

  def test_decrease_bid(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.decreaseBid) == RuleReportType.keyword

  def test_increase_cpa_goal(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.increaseCPAGoal) == RuleReportType.adGroup

  def test_decrease_cpa_goal(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.decreaseCPAGoal) == RuleReportType.adGroup

  def test_pause_keyword(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.pauseKeyword) == RuleReportType.keyword

  def test_no_action(self, channel: AppleSearchAdsChannel):
    assert channel.report_type(RuleActionType.noAction) == RuleReportType.keyword

  def test_pause_campaign(self, channel: AppleSearchAdsChannel):
    with pytest.raises(ValueError):
      channel.report_type(RuleActionType.pauseCampaign)

def test_rule_reporter(channel: AppleSearchAdsChannel):
  reporter = channel.rule_reporter(report_type=RuleReportType.campaign)
  assert isinstance(reporter, SearchAdsReporter)

@pytest.mark.incremental
class TestRuleActionIncreaseBid:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.increaseBid,
      adjustment_value=3.4,
      adjustment_limit=1.5
    )
  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsBidAction)

  def test_content(self):
    assert self.action.adjustmentValue == 1.034
    assert self.action.adjustmentLimit == 1.5

@pytest.mark.incremental
class TestRuleActionDecreaseBid:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.decreaseBid,
      adjustment_value=3.4,
      adjustment_limit=1.5
    )

  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsBidAction)

  def test_content(self):
    assert self.action.adjustmentValue == 0.966
    assert self.action.adjustmentLimit == 1.5

@pytest.mark.incremental
class TestRuleActionIncreaseCPAGoal:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.increaseCPAGoal,
      adjustment_value=6.2,
      adjustment_limit=3
    )

  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsCPAGoalAction)

  def test_content(self):
    assert self.action.adjustmentValue == 1.062
    assert self.action.adjustmentLimit == 3

@pytest.mark.incremental
class TestRuleActionDecreaseCPAGoal:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.decreaseCPAGoal,
      adjustment_value=6.2,
      adjustment_limit=2.4
    )

  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsCPAGoalAction)

  def test_content(self):
    assert self.action.adjustmentValue == 0.938
    assert self.action.adjustmentLimit == 2.4

@pytest.mark.incremental
class TestRuleActionPauseKeyword:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.pauseKeyword
    )

  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsPauseKeywordAction)

@pytest.mark.incremental
class TestRuleActionNoAction:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel):
    self.action = channel.rule_action(
      action_type=RuleActionType.noAction
    )

  def test_execute(self):
    assert self.action

  def test_shape(self):
    assert isinstance(self.action, SearchAdsNoAction)

@pytest.mark.incremental
class TestRuleActionPauseCampaign:
  def test_execute(self, channel: AppleSearchAdsChannel):
    with pytest.raises(ValueError):
      channel.rule_action(action_type=RuleActionType.pauseCampaign)

@pytest.mark.incremental
class TestGetEntitiesOrg:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.orgs = channel.get_entities(entity_type=ChannelEntity.org)
    yield

  def test_execute(self):
    assert self.orgs
  
  def test_shape(self, search_ads_account: SearchAdsAccount):
    assert len(self.orgs) == len(search_ads_account.get_orgs())
    assert set(self.orgs[0].keys()) == { 'id', 'name' }
  
  def test_content(self, search_ads_account: SearchAdsAccount):
    assert self.orgs[0]['id'] == '1'
    assert self.orgs[0]['name'] == 'y'
    assert self.orgs[1]['id'] == '2'
    assert self.orgs[1]['name'] == 'b'

@pytest.mark.incremental
class TestGetEntitiesCampaign:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.campaigns = channel.get_entities(entity_type=ChannelEntity.campaign, parent_ids={
        ChannelEntity.org: '1'
      })
    yield

  def test_execute(self):
    assert self.campaigns
  
  def test_shape(self, search_ads_api: SearchAds):
    assert len(self.campaigns) == len(search_ads_api.get_campaigns())
    assert set(self.campaigns[0].keys()) == { 'org_id', 'id', 'name' }
  
  def test_content(self, search_ads_account: SearchAdsAccount):
    assert self.campaigns[0]['org_id'] == '1'
    assert self.campaigns[0]['id'] == '10'
    assert self.campaigns[0]['name'] == 'b'
    assert self.campaigns[1]['org_id'] == '1'
    assert self.campaigns[1]['id'] == '11'
    assert self.campaigns[1]['name'] == 'y'

@pytest.mark.incremental
class TestGetEntitiesAdGroups:
  @pytest.fixture(autouse=True)
  def setup(self, channel: AppleSearchAdsChannel, credentials: Dict[str, any]):
    with channel.connected(credentials=credentials):
      self.campaigns = channel.get_entities(entity_type=ChannelEntity.ad_group, parent_ids={
        ChannelEntity.org: '1',
        ChannelEntity.campaign: '2'
      })
    yield

  def test_execute(self):
    assert self.campaigns
  
  def test_shape(self, search_ads_api: SearchAds):
    assert len(self.campaigns) == len(search_ads_api.get_adgroups())
    assert set(self.campaigns[0].keys()) == { 'campaign_id', 'org_id', 'id', 'name' }
  
  def test_content(self, search_ads_account: SearchAdsAccount):
    assert self.campaigns[0]['org_id'] == '1'
    assert self.campaigns[0]['campaign_id'] == '2'
    assert self.campaigns[0]['id'] == '20'
    assert self.campaigns[0]['name'] == 'b'
    assert self.campaigns[1]['org_id'] == '1'
    assert self.campaigns[1]['campaign_id'] == '2'
    assert self.campaigns[1]['id'] == '21'
    assert self.campaigns[1]['name'] == 'y'
    assert self.campaigns[2]['org_id'] == '1'
    assert self.campaigns[2]['campaign_id'] == '2'
    assert self.campaigns[2]['id'] == '22'
    assert self.campaigns[2]['name'] == 'v'

@pytest.mark.incremental
class TestGranularityIsCompatible:
  cases = [
    *[
      (False, f'hourly: forbid invalid report type ({report_type}) ', {
        'granularity': RuleReportGranularity.hourly,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=1),
        'end_date': datetime.utcnow(),
      })
      for report_type in [ RuleReportType.searchTerm ]
    ],
    *[
      (day_interval <= 7, f'hourly: interval is within 7 days ({report_type})', {
        'granularity': RuleReportGranularity.hourly,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_interval),
        'end_date': datetime.utcnow(),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword ]
      for day_interval in [6, 7, 8]
    ],
    *[
      (day_age <= 30, f'hourly: age is within 30 days ({report_type})', {
        'granularity': RuleReportGranularity.hourly,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_age),
        'end_date': datetime.utcnow() - timedelta(days=day_age-1),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword ]
      for day_age in [28, 29, 30, 31, 32]
    ],
    *[
      (day_interval <= 90, f'daily: interval is within 90 days ({report_type})', {
        'granularity': RuleReportGranularity.daily,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_interval),
        'end_date': datetime.utcnow(),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for day_interval in [88, 89, 90, 91, 92]
    ],
    *[
      (day_age <= 730, f'daily: age is within 730 days ({report_type})', {
        'granularity': RuleReportGranularity.daily,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_age),
        'end_date': datetime.utcnow() - timedelta(days=day_age-1),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for day_age in [-1, 0, 100, 728, 730, 731, 732, 750]
    ],
    *[
      (day_interval > 14, f'weekly: interval is more than 14 days ({report_type})', {
        'granularity': RuleReportGranularity.weekly,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_interval),
        'end_date': datetime.utcnow(),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for day_interval in [10, 11, 12, 13, 14, 15, 16, 17, 18, 100]
    ],
    *[
      (day_interval <= 365, f'weekly: interval is within 365 days ({report_type})', {
        'granularity': RuleReportGranularity.weekly,
        'report_type': report_type,
        'start_date': datetime.utcnow() - timedelta(days=day_interval),
        'end_date': datetime.utcnow(),
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for day_interval in [100, 200, 300, 350, 354, 365, 366, 367, 370, 380, 390, 400, 4000]
    ],
    *[
      (combo['output'], f'{granularity}: age is within 24 months ({report_type})', {
        'granularity': granularity,
        'report_type': report_type,
        'start_date': combo['start'],
        'end_date': combo['start'] + timedelta(days=120), # satisfies monthly and weekly interval constraints
      })
      for granularity in [ RuleReportGranularity.monthly, RuleReportGranularity.weekly ]
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for combo in [
        { 'output': False, 'start': datetime.utcnow() - timedelta(days=365 * 2 + 31) },
        { 'output': True, 'start': datetime.utcnow() - timedelta(days=365 * 2 - 31) }
      ]
    ],
    *[
      (combo['output'], f'monthly: interval is within 3 months ({report_type})', {
        'granularity': RuleReportGranularity.monthly,
        'report_type': report_type,
        'start_date': combo['start'],
        'end_date': combo['end']
      })
      for report_type in [ RuleReportType.campaign, RuleReportType.adGroup, RuleReportType.keyword, RuleReportType.searchTerm ]
      for combo in [
        { 'output': True, 'start': datetime.utcnow() - timedelta(days=365 * 2 - 31), 'end': datetime.utcnow() - timedelta(days=365 * 2 - 152) },
        { 'output': False, 'start': datetime.utcnow() - timedelta(days=365 * 2 - 31), 'end': datetime.utcnow() - timedelta(days=365 * 2 - 61) },
      ]
    ],
  ]

  @pytest.mark.parametrize('output, label, input', cases)
  def test(self, output, label, input, channel: AppleSearchAdsChannel):
    assert label and output is channel.granularity_is_compatible(**input)

