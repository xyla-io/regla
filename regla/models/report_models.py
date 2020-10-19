from __future__ import annotations
import bson
import pandas as pd
import numpy as np

from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict

class RuleReportColumn(Enum):
  campaign_id = 'campaignId'
  ad_group_id = 'adGroupId'
  date = 'date'
  spend = 'localSpend'
  impressions = 'impressions'
  clicks = 'taps'
  conversions = 'installs'

class RuleReportType(Enum):
  campaign = 'campaign'
  keyword = 'keyword'
  adGroup = 'adgroup'
  searchTerm = 'searchterm'

  @property
  def groupByID(self):
    if self is RuleReportType.keyword:
      return 'keywordId'
    elif self is RuleReportType.adGroup:
      return 'adGroupId'
    elif self is RuleReportType.searchTerm:
      return 'adGroupId'
    elif self is RuleReportType.campaign:
      return 'campaignId'
    else:
      raise ValueError('Unsupported report type', self)

  @property
  def historyTargetType(self):
    if self is RuleReportType.keyword:
      return 'keyword'
    elif self is RuleReportType.adGroup:
      return 'adgroup'
    elif self is RuleReportType.searchTerm:
      return 'adgroup'
    elif self is RuleReportType.campaign:
      return 'campaign'
    else:
      raise ValueError('Unsupported report type', self)

class RuleReportGranularity(Enum):
  hourly = 'HOURLY'
  daily = 'DAILY'
  weekly = 'WEEKLY'
  monthly = 'MONTHLY'

  @property
  def dateFormatString(self):
    if self is RuleReportGranularity.hourly:
      return '%Y-%m-%d %H'
    elif self is RuleReportGranularity.daily or self is RuleReportGranularity.weekly or self is RuleReportGranularity.monthly:
      return '%Y-%m-%d'
    else:
      raise ValueError('Unsupported report granularity', self)

class RuleReporter:
  reportType: Optional[RuleReportType]
  adGroupID: Optional[any]
  ruleID: Optional[bson.ObjectId]
  dataCheckRange: Optional[int]
  rawReport: Optional[pd.DataFrame]
  report: Optional[pd.DataFrame]
  context: Optional[any]
  fetch_raw_report_time: Optional[datetime]

  def __init__(self, reportType: Optional[RuleReportType]=None, adGroupID: Optional[any]=None, ruleID: Optional[bson.ObjectId]=None, dataCheckRange: Optional[int]=None, rawReport: Optional[pd.DataFrame]=None, report: Optional[pd.DataFrame]=None, context: Optional[any]=None, fetch_raw_report_time: Optional[datetime]=None):
    self.reportType = reportType
    self.adGroupID = adGroupID
    self.ruleID = ruleID
    self.dataCheckRange = dataCheckRange
    self.rawReport= rawReport
    self.report = report
    self.context = context
    self.fetch_raw_report_time = fetch_raw_report_time

  @property
  def rule_column_map(self) -> Dict[RuleReportColumn, str]:
    return {}

  def fetchRawReport(self, startDate, endDate, granularity, api, campaign, adGroupIDs=None):
    self.context = campaign
    self.fetch_raw_report_time = datetime.utcnow()
    self.rawReport = self._getRawReport(startDate=startDate, endDate=endDate, granularity=granularity, api=api, campaign=campaign, adGroupIDs=adGroupIDs)

  def filterRawReport(self, historyCollection):
    report = self.rawReport.copy()
    self._filterReport(report, historyCollection=historyCollection)
    self.report = report

  def processRawReportForImpact(self, historyCollection):
    report = self.rawReport.copy()
    self._processReportForImpact(report, historyCollection=historyCollection)
    self.report = report

  def _getRawReport(self, startDate, endDate, granularity, api, campaign, adGroupIDs):
    raise NotImplementedError()

  def _filterReport(self, report, historyCollection):
    if report.empty: return
    self._map_rule_columns(report=report)
    self._filter_future(report)
    self._filterProcessedData(report)
    self._filterByAdGroup(report)
    self._invalidateZeroDivisorData(report)
    self._filterByLastActionDate(report, historyCollection=historyCollection)

  def _filter_future(self, report):
    fetch_raw_report_hour = datetime(self.fetch_raw_report_time.year, self.fetch_raw_report_time.month, self.fetch_raw_report_time.day, self.fetch_raw_report_time.hour)
    report.drop(report.index[report.date >= fetch_raw_report_hour], inplace=True)

  def _processReportForImpact(self, report, historyCollection):
    if report.empty: return
    self._filterByAdGroup(report)
    self._invalidateZeroDivisorData(report)
    self._filterByActionTarget(report, historyCollection=historyCollection)

  def _filterProcessedData(self, report):
    if self.dataCheckRange is None: return

    maxDate = report.date.max()
    checkDate = maxDate - timedelta(milliseconds=self.dataCheckRange)

    report.drop(report.index[report.date <= checkDate], inplace=True)

  def _filterByAdGroup(self, report):
    if self.adGroupID is None: return

    report.drop(report.index[report.adGroupId != int(self.adGroupID)], inplace=True)

  def _filterByLastActionDate(self, report, historyCollection):
    if self.ruleID is None: return

    history = historyCollection.aggregate([
        {"$match": {"ruleID": self.ruleID, "targetType": self.reportType.historyTargetType, "consumedData": True}},
        {"$group": {"_id": "$targetID", "lastActionTakenDate": {"$max": "$lastDataCheckedDate"}}},
        {"$sort": {"lastActionTakenDate": -1}},
    ])

    for h in history:
        report.drop(report.index[(report[self.reportType.groupByID] == h["_id"]) & (report.date <= h["lastActionTakenDate"])],
                    inplace=True)

  def _filterByActionTarget(self, report, historyCollection):
      if self.ruleID is None: return

      report["actions"] = 0

      history = historyCollection.find({"ruleID": self.ruleID, "targetType": self.reportType.historyTargetType}, {"historyCreationDate": True, "targetID": True})

      for h in history:
          targetID = h["targetID"]
          historyDate = h["historyCreationDate"]
          targetRows = report.loc[(report.date <= historyDate) & (report[self.reportType.groupByID] == targetID)]
          if targetRows.empty: continue
          report.at[targetRows.date.idxmax(), 'actions'] += 1

      report["totalActions"] = report["actions"].groupby(report[self.reportType.groupByID]).transform('sum')
      report.drop(
          report.index[report.totalActions == 0],
          inplace=True)

  def _invalidateZeroDivisorData(self, report):
      report.loc[report.taps == 0, "avgCPT"] = np.nan
      report.loc[report.installs == 0, "avgCPA"] = np.nan

  def _map_rule_columns(self, report: pd.DataFrame):
    for rule_column, column in self.rule_column_map.items():
      report[rule_column.value] = report[column] if column in report.columns else None
  