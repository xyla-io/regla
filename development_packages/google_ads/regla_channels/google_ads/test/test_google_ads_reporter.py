import pytest
import pandas as pd
import pathlib

from datetime import datetime, timedelta
from typing import Dict
from unittest import mock
from ..google_ads_reporter import GoogleAdsReporter
from hazel import GoogleAdsReporter as HazelReporter, GoogleAdsAPI as HazelAPI
from regla import RuleReportType, RuleReportColumn

parent_directory = pathlib.Path(__file__).parent.absolute()
sample_google_ads_performance_report_csv_path = f'{parent_directory}/sample/csv/google_ads_performance_report.csv'

@pytest.fixture
def reporter() -> GoogleAdsReporter:
  yield GoogleAdsReporter()

@pytest.fixture(autouse=True)
def hazel_reporter() -> HazelReporter:
  with mock.patch.object(HazelReporter, 'get_performance_report', return_value=pd.read_csv(sample_google_ads_performance_report_csv_path)), \
    mock.patch.object(HazelReporter, '__init__', return_value=None):
    yield HazelReporter(api=None)

class TestEntityGranularity:
  def test_entity_granularity_campaign(self, reporter: GoogleAdsReporter):
    reporter.reportType = RuleReportType.campaign
    assert reporter.entity_granularity == 'campaign'

  def test_entity_granularity_ad_group(self, reporter: GoogleAdsReporter):
    reporter.reportType = RuleReportType.adGroup
    assert reporter.entity_granularity == 'ad_group'

def test_rule_column_map(reporter: GoogleAdsReporter):
  column_map = reporter.rule_column_map
  assert column_map[RuleReportColumn.campaign_id] == 'campaign_id'
  assert column_map[RuleReportColumn.impressions] == 'metrics_impressions'
  assert column_map[RuleReportColumn.clicks] == 'metrics_clicks'
  assert column_map[RuleReportColumn.conversions] == 'metrics_conversions'

def test_time_granularity(reporter: GoogleAdsReporter):
  assert reporter.time_granularity('HeLLO_0') == 'hello_0'

class TestGoogleAdsReporterReport:
  @pytest.fixture(autouse=True)
  def setup(self, reporter: GoogleAdsReporter):
    self.reporter = reporter
    self.reporter.reportType = RuleReportType.campaign
    self.reporter.fetchRawReport(
      startDate=datetime.utcnow() - timedelta(days=4),
      endDate=datetime.utcnow(),
      granularity='hourly',
      api=None,
      campaign={'campaign_id': '123'},
    )

  @pytest.fixture
  def history_collection(self):
    h = mock.Mock()
    h.aggregate = mock.Mock(return_value=[])
    yield h

  def test_fetchRawReport_execute(self):
    assert not self.reporter.rawReport.empty

  def test_filterRawReport_execute(self, history_collection):
    self.reporter.filterRawReport(history_collection)
    assert not self.reporter.report.empty

  def test_filterRawReport_shape(self, history_collection):
    self.reporter.filterRawReport(history_collection)
    required_columns = [
      'campaignId',
      'date',
      'localSpend',
      'impressions',
      'taps',
      'installs',
    ]
    for column in required_columns:
      assert column in self.reporter.report.columns
