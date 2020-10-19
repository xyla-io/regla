import pandas as pd
import pytest

from typing import Dict
from unittest import mock
from hazel import GoogleAdsReporter as HazelReporter, GoogleAdsMutator as HazelMutator, GoogleAdsAPI as HazelAPI
from regla import RuleActionTargetType
from ..google_ads_actions import GoogleAdsPauseCampaignAction

@pytest.fixture
def pause_campaign_action() -> GoogleAdsPauseCampaignAction:
  return GoogleAdsPauseCampaignAction()

@pytest.fixture(params=[0, 4, 99, 100, 101, 200])
def hazel_reporter(request) -> HazelReporter:
  with mock.patch.object(HazelReporter, 'get_safety_report', side_effect=lambda **kwargs: pd.DataFrame(data=[{
      'customer#descriptive_name': '',
      'customer#currency_code': '',
      'campaign#id': '1',
      'campaign#name': '',
      'metrics#conversions': request.param,
    }])), \
    mock.patch.object(HazelReporter, '__init__', return_value=None):
    yield HazelReporter(api=None)

@pytest.fixture(autouse=True)
def hazel_mutator() -> HazelMutator:
  HazelMutator.did_pause_campaign = False

  def pause_campaign_side_effect(*args, **kwargs):
    HazelMutator.did_pause_campaign = True

  with mock.patch.object(HazelMutator, 'pause_campaign', side_effect=pause_campaign_side_effect),\
    mock.patch.object(HazelMutator, '__init__', return_value=None):
    yield HazelMutator(api=None)

@pytest.fixture
def hazel_api() -> HazelAPI:
  with mock.patch.object(HazelAPI, 'response_to_record', return_value='mock_api_response'):
    yield HazelAPI

def test_entity_granularity(pause_campaign_action: GoogleAdsPauseCampaignAction):
  assert pause_campaign_action.entity_granularity is RuleActionTargetType.campaign

@pytest.mark.usefixtures('hazel_reporter')
class TestGenerateSafetyReport:
  def test_execute(self, pause_campaign_action: GoogleAdsPauseCampaignAction):
    pause_campaign_action.generate_safety_report(api=None, report=pd.DataFrame([{ 'campaignId': '1'}]))

  def test_shape(self, pause_campaign_action: GoogleAdsPauseCampaignAction):
    safety_report = pause_campaign_action.generate_safety_report(api=None, report=pd.DataFrame([{ 'campaignId': '1'}]))
    expected_columns = ['customer_descriptive_name', 'customer_currency_code', 'campaign_id', 'campaign_name', 'metrics_conversions', 'protection_messages']
    for column in expected_columns:
      assert column in safety_report.columns
    if (len(safety_report.metrics_conversions) > 0 and safety_report.metrics_conversions[0] < 100):
      assert len(safety_report.protection_messages[0]) > 0

class TestGoogleAdsActionsAdjust:
  def test_adjust_empty_report(self, pause_campaign_action: GoogleAdsPauseCampaignAction):
    result = pause_campaign_action.adjust(
      api=None,
      campaign={'campaign_id': '1', 'safe_mode': True},
      report=pd.DataFrame([]),
      dryRun=False
    )
    assert len(result.report.columns) == 0
    assert len(result.report.index) == 0
  
  def test_adjust_campaign_id_mismatch(self, pause_campaign_action: GoogleAdsPauseCampaignAction, hazel_api):
    with pytest.raises(AssertionError):
      pause_campaign_action.adjust(
        api=hazel_api,
        campaign={'campaign_id': '1', 'safe_mode': True},
        report=pd.DataFrame([{'campaignId': '2'}]),
        dryRun=False
      )

  def test_adjust_safe_mode_on(self, pause_campaign_action: GoogleAdsPauseCampaignAction, hazel_reporter: HazelReporter, hazel_api: HazelAPI):
    result = pause_campaign_action.adjust(
      api=hazel_api,
      campaign={'campaign_id': '1', 'safe_mode': True},
      report=pd.DataFrame([{'campaignId': '1', 'campaign_name': ''}]),
      dryRun=False
    )
    assert len(result.apiResponse) == 1
    assert not result.errors
    if hazel_reporter.get_safety_report()['metrics#conversions'][0] < 100:
      assert not HazelMutator.did_pause_campaign
      assert result.apiResponse[0] == None
    else:
      assert HazelMutator.did_pause_campaign
      assert result.apiResponse[0] == 'mock_api_response'
    
  def test_adjust_safe_mode_off(self, pause_campaign_action: GoogleAdsPauseCampaignAction, hazel_reporter: HazelReporter, hazel_api: HazelAPI):
    result = pause_campaign_action.adjust(
      api=hazel_api,
      campaign={'campaign_id': '1', 'safe_mode': False},
      report=pd.DataFrame([{'campaignId': '1', 'campaign_name': ''}]),
      dryRun=False
    )
    assert len(result.apiResponse) == 1
    assert not result.errors
    assert HazelMutator.did_pause_campaign
    assert result.apiResponse[0] == 'mock_api_response'
