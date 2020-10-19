import pandas as pd

from regla import RuleReportColumn, RuleReporter, RuleReportType, RuleContext, RuleOption
from hazel import GoogleAdsReporter as HazelReporter
from typing import Dict
from .google_ads_context import GoogleAdsContext, GoogleAdsOption, add_report_time

class GoogleAdsReporter(RuleReporter):
  @property
  def entity_granularity(self) -> str:
    if self.reportType is RuleReportType.campaign:
      return 'campaign'
    elif self.reportType is RuleReportType.adGroup:
      return 'ad_group'
    else:
      raise ValueError('Unsupported google ads report type', self.reportType)

  @property
  def rule_column_map(self) -> Dict[RuleReportColumn, str]:
    return {
      RuleReportColumn.campaign_id: 'campaign_id',
      RuleReportColumn.impressions: 'metrics_impressions',
      RuleReportColumn.clicks: 'metrics_clicks',
      RuleReportColumn.conversions: 'metrics_conversions',
    }

  def time_granularity(self, granularity: str) -> str:
    return granularity.lower()

  def _map_rule_columns(self, report: pd.DataFrame):
    report.rename(lambda s: s.replace('#', '_'), axis='columns', inplace=True)
    report[RuleReportColumn.spend.value] = report['metrics_cost_micros'].apply(lambda c: c / 1000000 if not pd.isna(c) else c) if 'metrics_cost_micros' in report.columns else None
    add_report_time(
      report=report,
      time_column=RuleReportColumn.date.value
    )
    assert report.customer_currency_code.unique() == ['USD'], 'Only accounts using USD currency are currently supported'
    
    super()._map_rule_columns(report=report)
    if self.context[RuleContext.rule_options.value][GoogleAdsOption.use_optimized_conversions.value] and 'campaign_selective_optimization_conversion_actions' in report.columns:
      location = report.loc[report.campaign_selective_optimization_conversion_actions.notna()]
      report.loc[location.index, RuleReportColumn.conversions.value] = location.selected_conversions

  def _getRawReport(self, startDate, endDate, granularity, api, campaign, adGroupIDs):
    reporter = HazelReporter(api=api)
    report = reporter.get_performance_report(
      start_date=startDate,
      end_date=endDate,
      entity_granularity=self.entity_granularity,
      time_granularity=self.time_granularity(granularity=granularity),
      entity_ids=[campaign[GoogleAdsContext.campaign_id.value]]
    )
    if self.context[RuleContext.rule_options.value][GoogleAdsOption.use_optimized_conversions.value]:
      report = reporter.add_selected_conversions(
        report=report,
        start_date=startDate,
        end_date=endDate,
        entity_granularity=self.entity_granularity,
        time_granularity=self.time_granularity(granularity=granularity)
      )
    return report

  def _filterByLastActionDate(self, report, historyCollection):
    if not self.context[RuleContext.rule_options.value][RuleOption.dynamic_window.value]:
      return
    return super()._filterByLastActionDate(
      report=report,
      historyCollection=historyCollection
    )
