from bson import ObjectId
import json
import pandas as pd
import pdb
from datetime import datetime, timedelta
from functools import reduce
from math import floor
from typing import List, Dict, Optional

from .context_models import RuleContext, RuleOption
from .action_types import RuleActionType
from .report_models import RuleReporter, RuleReportGranularity
from .condition_models import RuleKPI, RuleConditionGroup
from moda.connect import Connector
from .channel_models import Channel
from ..factories import channel_factory

class RuleConnection:
  options: Dict[str, any]

  def __init__(self, options: Dict[str, any]):
    self.options = options

  @property
  def channel(self) -> Channel:
    return self.options[RuleContext.channel.value]

  @property
  def api(self) -> any:
    return self.channel.api

  @property
  def history_collection(self) -> any:
    return self.options[RuleContext.history_collection.value]

  @property
  def monitor_collection(self) -> any:
    return self.options[RuleContext.monitor_collection.value]

  @property
  def channel_context(self) -> any:
    return self.options[RuleContext.channel_context.value]

class Rule(Connector):
    channel_identifier: Optional[str]
    orgID: Optional[any]
    campaignID: Optional[any]
    adgroupID: Optional[any]
    userID: Optional[ObjectId]
    ruleID: Optional[ObjectId]
    acciont: Optional[str]
    metadata: Optional[Dict[str, any]]
    tasks: Optional[List[Dict[str, any]]]
    dryRun: bool
    monitor: bool
    safe_mode: bool
    dataCheckRange: Optional[int]
    created: Optional[datetime]
    options: Optional[Dict[str, any]]

    def __init__(self,
                 channel_identifier=None,
                 orgID=None,
                 campaignID=None,
                 adgroupID=None,
                 userID=None,
                 ruleID=None,
                 account=None,
                 metadata=None,
                 tasks=None,
                 dryRun=False,
                 monitor=False,
                 safe_mode=True,
                 dataCheckRange=None,
                 created=None,
                 options=None):
        self._id = ruleID
        self.userID = userID
        self.channel_identifier = channel_identifier
        self.account = account
        self.campaignID = campaignID
        self.metadata = metadata
        self.tasks = tasks
        self.dataCheckRange = dataCheckRange
        self.orgID = orgID
        self.adgroupID = adgroupID
        self.dryRun = dryRun
        self.monitor = monitor
        self.safe_mode = safe_mode
        self.created = created
        self.options = options

    @classmethod
    def ruleWithID(cls, rulesCollection, conditionGroupsCollection, id):
        data = rulesCollection.find_one({"_id": ObjectId(id)})
        channel = channel_factory(channel_identifier=data['channel'])
        tasks = [RuleTask.taskWithDBRepresentation(channel, t, conditionGroupsCollection=conditionGroupsCollection) for t in data["tasks"]]

        rule = cls(
            channel_identifier=data['channel'],
            orgID=data["orgID"],
            campaignID=data["campaignID"],
            adgroupID=data["adgroupID"],
            userID=str(data['user']),
            ruleID=id,
            account=data["account"],
            metadata=data["metadata"],
            tasks=tasks,
            dryRun=(data["shouldPerformAction"] is False),
            monitor=data["shouldMonitor"],
            safe_mode=data['safeMode'],
            dataCheckRange=data["dataCheckRange"],
            created=data["created"],
            options=data['options'] if 'options' in data else {}
        )

        return rule

    def __repr__(self):
        return "Rule {id} (tasks: {tasks})".format(id=self._id, tasks=self.tasks)

    def connect(self, credentials: Optional[any]=None, rule_collection: Optional[any]=None, history_collection: Optional[any]=None, monitor_collection: Optional[any]=None, options: Dict[str, any]={}):
      channel = channel_factory(channel_identifier=self.channel_identifier)
      if credentials is not None:
        channel.connect(credentials=credentials)
      connection_options = {
        RuleContext.now.value: datetime.utcnow(),
        RuleContext.rule.value: self,
        RuleContext.rule_options.value: {
          **RuleOption.get_defaults(),
          **self.options,
        },
        RuleContext.channel.value: channel,
        RuleContext.rule_collection.value: rule_collection,
        RuleContext.history_collection.value: history_collection,
        RuleContext.monitor_collection.value: monitor_collection,
        **options,
      }
      self.connection = RuleConnection(options={
        **connection_options,
        RuleContext.channel_context.value: channel.rule_context(options=connection_options),
      })

    def disconnect(self):
      self.connection.channel.disconnect()
      self.connection = None

    def getReporters(self, startDate, endDate, granularity=None, processor=None):
        if self.connection.api is None:
            raise ValueError("api property is None in Rule", self)

        allReportTypes = [self.connection.channel.report_type(action_type=a.type) for t in self.tasks for a in t.actions]
        reportTypes = []
        [reportTypes.append(t) for t in allReportTypes if not reportTypes.count(t)]

        reporters = {}
        for reportType in reportTypes:
            report_granularity = self.connection.channel.highest_compatible_granularity(
              report_type=reportType, 
              start_date=startDate, 
              end_date=endDate) if granularity is None else granularity
            reporter = self.connection.channel.rule_reporter(
              report_type=reportType,
              ad_group_id=self.adgroupID,
              rule_id=ObjectId(self._id),
              data_check_range=self.dataCheckRange
            )
            reporter.fetchRawReport(startDate=startDate, endDate=endDate, granularity=report_granularity, api=self.connection.api, campaign=self.connection.channel_context)
            if processor is not None:
                processor(reporter)
            reporters[reportType.value] = reporter

        return reporters
    
    def impactReportMetadata(self):
      return RuleImpactReportMetadata(rule=self)

    def getImpactReport(self):
      metadata = self.impactReportMetadata()
      if not metadata.is_valid: return None

      reporters: List[RuleReporter] = self.getReporters(startDate=metadata.start_date, endDate=metadata.end_date, granularity=metadata.granularity.value, processor=lambda reporter : reporter.processRawReportForImpact(historyCollection=self.connection.history_collection))

      report = reduce(lambda r1, r2: r1.append(r2, sort=True), [reporters[k].report for k in reporters])

      return report

    def execute(self, startDate, endDate, granularity, debugEndDate=None):
        reporters = self.getReporters(startDate=startDate, endDate=endDate, granularity=granularity, processor=lambda reporter : reporter.filterRawReport(historyCollection=self.connection.history_collection))

        if debugEndDate is not None:
            for key in reporters:
                reporter = reporters[key]
                reporter.report.drop(reporter.report.index[reporter.report.date > debugEndDate], inplace=True)

        results = []
        finalReport = None
        monitorInfo = []
        for t in self.tasks:
            action = t.actions[0]
            if len(t.actions) > 1:
                raise ValueError("Multiple search ads actions per task are not supported", t)

            reporter = reporters[self.connection.channel.report_type(action_type=action.type).value]
            report = reporter.report
            reportCopy = report.copy()
            t.conditionGroup.filterData(reportCopy, groupByID=self.connection.channel.report_type(action_type=action.type).groupByID)
            result = action.adjust(api=self.connection.api, campaign=self.connection.channel_context, report=reportCopy, dryRun=self.dryRun)

            self._logHistory(
              reportCopy,
              logs=list(filter(lambda l: l is not None, result.logs)),
              errors=list(filter(lambda e: e is not None, result.errors)),
              history_collection=self.connection.history_collection
            )
            if self.monitor:
                monitorInfo.append({
                  'report': reportCopy.to_csv(),
                  'sourceReport': report.to_csv(),
                  'action_report': result.action_report.to_csv() if result.action_report is not None else None,
                  'apiResponse': result.apiResponse,
                })

            report.drop(reportCopy.index, inplace=True)
            results.append(result)
            if finalReport is None:
                finalReport = reportCopy.copy()
            else:
                finalReport = pd.concat([finalReport, reportCopy], sort=True)

        if self.monitor:
            self._logMonitorInfo(monitorInfo=monitorInfo)

        return RuleResult(report=finalReport,
                                   actionResults=results)

    def _logHistory(self, report, logs, errors, history_collection):
        now = datetime.utcnow()
        description = "{channel} ({orgID}) → {campaign} → {adGroup} | {description}".format(channel=self.connection.channel.title, orgID=self.orgID, campaign=self.metadata["campaignName"], adGroup=self.metadata["adGroupName"], description=self.metadata["description"])
        rule_metadata = {"userID": ObjectId(self.userID), "ruleID": ObjectId(self._id), "historyCreationDate": now, "ruleDescription": description, "dryRun": self.dryRun}
        if not report.empty:
          rule_metadata['lastDataCheckedDate'] = report.date.max()
        history = [{**l.dbRepresentation, **rule_metadata} for l in logs]
        if errors:
          history.append({
            'historyType': 'error',
            'targetID': -1,
            'actionDescription': f'<strong>ERROR:</strong> {len(errors)} error{"s" if len(errors) != 1 else ""} occurred while attempting the last {len(logs)} action{"s" if len(logs) != 1 else ""} for rule {description}',
            'errorDescriptions': [repr(e) for e in errors],
            **rule_metadata,
          })
        history.append({
          'historyType': 'execute',
          'targetID': -1,
          'actionCount': len(logs),
          'actionDescription': f'Attempting {len(logs)} action{"s" if len(logs) != 1 else ""} for rule {description}',
          **rule_metadata,
        })
        history_collection.insert_many(history)

    def _logMonitorInfo(self, monitorInfo):
        if not monitorInfo: return

        now = datetime.utcnow()
        log = {"ruleID": ObjectId(self._id), "logCreationDate": now, "ruleDescription": self.metadata["description"], "tasks": monitorInfo}
        self.connection.monitor_collection.insert_one(log)


class RuleTask(object):
    def __init__(self,
                 conditionGroup=None,
                 actions=None):
        self.conditionGroup = conditionGroup
        self.actions = actions

    @classmethod
    def taskWithDBRepresentation(cls, channel, data, conditionGroupsCollection):
        conditionGroup = RuleConditionGroup.groupWithID(conditionGroupsCollection=conditionGroupsCollection, id=str(data["conditionGroup"]))
        deserializer = RuleActionDeserializer(channel=channel)
        actions = [deserializer.default(a) for a in data["actions"]]
        return cls(conditionGroup=conditionGroup,
                   actions=actions)


class RuleResult(object):
    def __init__(self,
                 report=None,
                 actionResults=None):
        self.report = report
        self.actionResults = actionResults

    def serialize_result(self):
        return {
            "report": self.report.to_csv() if self.report is not None and not self.report.empty else "",
            "actionResults": self.actionResults,
        }

class RuleImpactReportMetadata:
  start_date = None
  end_date = None
  granularity = None

  def __init__(self, rule):
    creation_day = datetime(rule.created.year, rule.created.month, rule.created.day)
    now = datetime.utcnow()
    yesterday = datetime(now.year, now.month, now.day) - timedelta(days=1)
    age_days = (yesterday - creation_day).days
    
    self.start_date = min(yesterday, creation_day - timedelta(days=age_days))
    self.end_date = yesterday

    self.granularity = self._highestGranularity(rule=rule, start_date=self.start_date, end_date=self.end_date)

    if self.granularity is RuleReportGranularity.weekly:
      creation_week = creation_day - timedelta(days=creation_day.weekday())
      week_age = floor(((yesterday - creation_week).days + 1) / 7)
      self.start_date = creation_week - timedelta(days=(week_age - 1) * 7)
      self.end_date = creation_week + timedelta(days=(week_age - 1) * 7)
      self.granularity = self._highestGranularity(rule=rule, start_date=self.start_date, end_date=self.end_date)

    if self.granularity is RuleReportGranularity.monthly:
      creation_month = datetime(year=creation_day.year, month=creation_day.month, day=1)
      month_age = (yesterday.year - creation_month.year) * 12 + yesterday.month - creation_day.month
      self.start_date = self._monthDelta(date=creation_month, delta=-(month_age - 1))
      self.end_date = self._monthDelta(date=creation_month, delta=month_age - 1)
      self.granularity = self._highestGranularity(rule=rule, start_date=self.start_date, end_date=self.end_date)

    print(json.dumps({'log': 'start date {start}, end date {end}'.format(start=self.start_date, end=self.end_date)}))

  def _monthDelta(self, date, delta):
    month = (date.month + delta - 1) % 12 + 1
    year = date.year + floor((date.month + delta - 1) / 12)
    return datetime(year=year, month=month, day=date.day)

  def _highestGranularity(self, rule, start_date, end_date):
    if start_date > end_date: return None

    report_types = set([rule.connection.channel.report_type(action_type=a.type) for t in rule.tasks for a in t.actions])
    granularities = [rule.connection.channel.highest_compatible_granularity(report_type=r, start_date=self.start_date, end_date=self.end_date) for r in report_types]

    if None in granularities: return None 
    
    granularity = None
    for g in RuleReportGranularity:
      if g in granularities: 
        granularity = g

    if granularity is RuleReportGranularity.hourly: 
      granularity = RuleReportGranularity.daily

    return granularity

  @property
  def is_valid(self):
    return self.granularity is not None

class RuleActionDeserializer(json.JSONDecoder):
  channel: Channel

  def __init__(self, channel: Channel):
    self.channel = channel
    super().__init__()

  def decode(self, s):
    o = super().decode(s)
    return self.default(o)

  def default(self, o):
    action = self.channel.rule_action(
      action_type=RuleActionType(o["action"]),
      adjustment_value=o["adjustmentValue"],
      adjustment_limit=o["adjustmentLimit"]
    )
    return action

