import pandas as pd

from typing import Type, List, Dict, Optional
from io_map import IOMap, IOSingleSourceReporter
from regla import MapReporter, RawReporter, RuleReportType, RuleReportColumn, RuleReportGranularity, RuleContext
from azrael import SnapchatReporter as AzraelReporter, SnapchatAPI as AzraelAPI
from datetime import timedelta, datetime
from math import ceil
from .snapchat_context import SnapchatContext, convert_time_series_to_utc

class SnapchatReporter(MapReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'snapchat_reporter'

  @property
  def raw_reporter_class(self) -> Type[IOMap]:
    if self.reportType is RuleReportType.campaign:
      return SnapchatRawCampaignReporter
    elif self.reportType is RuleReportType.adGroup:
      return SnapchatRawAdSquadReporter
    else:
      raise ValueError('Unsupported snapchat report type', self.reportType)

  @property
  def raw_columns(self) -> List[str]:
    return [
      'impressions',
      'swipes',
      'spend',
    ]    

  @property
  def rule_column_map(self) -> Dict[RuleReportColumn, str]:
    column_map = {
      RuleReportColumn.impressions: 'impressions',
      RuleReportColumn.clicks: 'swipes',
    }
    if self.reportType is RuleReportType.campaign:
      column_map[RuleReportColumn.campaign_id] = 'campaign_id'
    elif self.reportType is RuleReportType.adGroup:
      column_map[RuleReportColumn.ad_group_id] = 'adsquad_id'
    return column_map

  def _map_rule_columns(self, report: pd.DataFrame):
    report[RuleReportColumn.conversions.value] = None
    report[RuleReportColumn.date.value] = convert_time_series_to_utc(report['start_time'])
    report[RuleReportColumn.spend.value] = report.spend / 1000000
    super()._map_rule_columns(report=report)

class SnapchatRawReporter(RawReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'snapchat_raw_reporter'

  @property
  def entity_granularity(self) -> str:
    raise NotImplementedError()

  @property
  def entity_ids(self) -> str:
    raise NotImplementedError()

  @property
  def entity_columns(self) -> List[str]:
    return [
      'name',
      'status',
      'daily_budget_micro',
    ]

  @property
  def time_granularity(self) -> str:
    if self.granularity == RuleReportGranularity.hourly.value:
      return 'HOUR'
    elif self.granularity == RuleReportGranularity.daily.value:
      return 'DAY'
    else:
      raise ValueError('Unsupported granularity', self.granularity)

  def fetch_raw_report(self) -> pd.DataFrame:
    reporter = AzraelReporter(api=self.api)
    columns = list(filter(lambda c: c not in self.entity_columns, self.columns))
    entity_columns = list(filter(lambda c: c in self.entity_columns, self.columns))

    if self.start_date is None and self.end_date is None:
      assert not columns
      return reporter.get_performance_report(
        time_granularity=self.time_granularity,
        entity_granularity=self.entity_granularity,
        entity_ids=self.entity_ids,
        entity_columns=entity_columns
      )

    # TODO: Support daily granulairity if necessary by converting dates to the account's time zone
    start = self.start_date
    end = self.end_date + timedelta(days=1)

    # Snapchat only supports a 7 day interval when retriving hourly stats
    report_interval = timedelta(seconds=60 * 60 * 24 * 7)
    periods = ceil((end - start) / report_interval)
    report = None

    for period in range(periods):
      period_start = start + report_interval * period
      period_end = min(period_start + report_interval, end)
      period_report = reporter.get_performance_report(
        time_granularity=self.time_granularity,
        entity_granularity=self.entity_granularity,
        entity_ids=self.entity_ids,
        start_date=period_start,
        end_date=period_end,
        columns=columns,
        entity_columns=entity_columns
      )
      if report is None:
        report = period_report
      else:
        report = report.append(period_report)

    report.reset_index(drop=True, inplace=True)
    return report

class SnapchatRawCampaignReporter(SnapchatRawReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'snapchat_raw_campaign_reporter'

  @property
  def entity_granularity(self) -> str:
    return 'campaign'

  @property
  def entity_ids(self) -> Optional[List[str]]:
    return [self.context[SnapchatContext.campaign_id.value]] if SnapchatContext.campaign_id.value in self.context else None

class SnapchatRawAdSquadReporter(SnapchatRawReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'snapchat_raw_adsquad_reporter'

  @property
  def entity_granularity(self) -> str:
    return 'adsquad'