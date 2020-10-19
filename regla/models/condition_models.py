import numpy as np
import pandas as pd

from enum import Enum
from bson import ObjectId
from typing import List, Dict
from io_map import IOMap, IOMapKey, AllMap

class RuleKPI(Enum):
    cpa = 'reavgCPA'
    cpt = 'reavgCPT'
    spend = 'totalSpend'
    impressions = 'totalImpressions'
    taps = 'totalTaps'
    ttr = 'reavgTTR'
    conversions = 'totalConversions'
    conversionRate = 'reavgConversionRate'
    cpm = 'reavgCPM'

    def _calculateTotalSpend(self, report, groupByID):
        if "totalSpend" in report.columns: return

        report["totalSpend"] = report.groupby(groupByID).localSpend.transform(sum)

    def _calculateTotalImpressions(self, report, groupByID):
        if "totalImpressions" in report.columns: return

        report["totalImpressions"] = report.groupby(groupByID).impressions.transform(sum)

    def _calculateTotalTaps(self, report, groupByID):
        if "totalTaps" in report.columns: return

        report["totalTaps"] = report.groupby(groupByID).taps.transform(sum)

    def _calculateTotalConversions(self, report, groupByID):
        if "totalConversions" in report.columns: return

        report["totalConversions"] = report.groupby(groupByID).installs.transform(sum)

    def _reaverageCPT(self, report):
        if "reavgCPT" in report.columns: return

        report["reavgCPT"] = report.totalSpend / report.totalTaps
        report.loc[report.reavgCPT == np.inf, "reavgCPT"] = np.nan

    def _reaverageCPA(self, report):
        if "reavgCPA" in report.columns: return

        report["reavgCPA"] = report.totalSpend / report.totalConversions
        report.loc[report.reavgCPA == np.inf, "reavgCPA"] = np.nan

    def _reaverageTTR(self, report):
        if "reavgTTR" in report.columns: return

        report["reavgTTR"] = report.totalTaps / report.totalImpressions
        report.loc[report.reavgTTR == np.inf, "reavgTTR"] = np.nan

    def _reaverageConversionRate(self, report):
        if "reavgConversionRate" in report.columns: return

        report["reavgConversionRate"] = report.totalConversions / report.totalTaps
        report.loc[report.reavgConversionRate == np.inf, "reavgConversionRate"] = np.nan

    def _reaverage_cpm(self, report):
      if RuleKPI.cpm.value in report.columns: return

      report[RuleKPI.cpm.value] = report[RuleKPI.spend.value] / (report[RuleKPI.impressions.value] / 1000)
      report.loc[report[RuleKPI.cpm.value] == np.inf, RuleKPI.cpm.value] = np.nan

    def addRequiredColumns(self, report, groupByID):
        if self is RuleKPI.cpa:
            self._calculateTotalSpend(report, groupByID=groupByID)
            self._calculateTotalConversions(report, groupByID=groupByID)
            self._reaverageCPA(report)
        elif self is RuleKPI.cpt:
            self._calculateTotalSpend(report, groupByID=groupByID)
            self._calculateTotalTaps(report, groupByID=groupByID)
            self._reaverageCPT(report)
        elif self is RuleKPI.spend:
            self._calculateTotalSpend(report, groupByID=groupByID)
        elif self is RuleKPI.impressions:
            self._calculateTotalImpressions(report, groupByID=groupByID)
        elif self is RuleKPI.taps:
            self._calculateTotalTaps(report, groupByID=groupByID)
        elif self is RuleKPI.conversions:
            self._calculateTotalConversions(report, groupByID=groupByID)
        elif self is RuleKPI.ttr:
            self._calculateTotalImpressions(report, groupByID=groupByID)
            self._calculateTotalTaps(report, groupByID=groupByID)
            self._reaverageTTR(report)
        elif self is RuleKPI.conversionRate:
            self._calculateTotalTaps(report, groupByID=groupByID)
            self._calculateTotalConversions(report, groupByID=groupByID)
            self._reaverageConversionRate(report)
        elif self is RuleKPI.cpm:
            self._calculateTotalSpend(report, groupByID=groupByID)
            self._calculateTotalImpressions(report, groupByID=groupByID)
            self._reaverage_cpm(report)
        else:
            raise ValueError("unsupported KPI", self)

    def selectedIndex(self, report, operator, value):
        if (self is RuleKPI.cpt or self is RuleKPI.cpa) and (operator is RuleConditionalOperator.greater or operator is RuleConditionalOperator.greaterThanOrEqual):
            selectedIndices = operator.selectedIndex(report, column=self.value, value=value)
            nanReport = report[np.isnan(report[self.value])]
            selectedIndices = selectedIndices.union(operator.selectedIndex(nanReport, column=RuleKPI.spend.value, value=value))
            return selectedIndices
        else:
            return operator.selectedIndex(report, column=self.value, value=value)


class RuleConditionalOperator(Enum):
    less = 'less'
    greater = 'greater'
    lessThanOrEqual = 'leq'
    greaterThanOrEqual = 'geq'
    equal = 'equal'

    def selectedIndex(self, report, column, value):
        if self is RuleConditionalOperator.less:
            return report.index[report[column] < value]
        elif self is RuleConditionalOperator.greater:
            return report.index[report[column] > value]
        elif self is RuleConditionalOperator.lessThanOrEqual:
            return report.index[report[column] <= value]
        elif self is RuleConditionalOperator.greaterThanOrEqual:
            return report.index[report[column] >= value]
        elif self is RuleConditionalOperator.equal:
            return report.index[report[column] == value]
        else:
            raise ValueError("unsupported conditional operator", self)


class RuleCondition(IOMap):
  def __init__(self,
                kpi=None,
                operator=None,
                comparisonValue=None):
    self.kpi = kpi
    self.operator = operator
    self.comparisonValue = comparisonValue

  def selectedIndex(self, report, groupByID):
    self.kpi.addRequiredColumns(report, groupByID)
    return self.kpi.selectedIndex(report, operator=self.operator, value=self.comparisonValue)

  #----------
  # IOMap
  #----------
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'condition'

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return ['selected_index']

  def run(self, report: pd.DataFrame, group_by_id: str) -> Dict[str, any]:
    super().run(
      report=report,
      group_by_id=group_by_id
    )
    self.selected_index = self.selectedIndex(
      report=report,
      groupByID=group_by_id
    )
    return self.populated_output

class RuleConditionGroupOperator(Enum):
    any = 'any'
    all = 'all'

    def combineIndicies(self, first, second):
        if self is RuleConditionGroupOperator.all:
            return first.intersection(second)
        elif self is RuleConditionGroupOperator.any:
            return first.union(second)
        else:
            raise ValueError("unsupported condition group operator", self)

    def undeterminedData(self, report, selectedIndex=None):
        if self is RuleConditionGroupOperator.all:
            return report.loc[selectedIndex]
        elif self is RuleConditionGroupOperator.any:
            return report.loc[report.index.difference(selectedIndex)]
        else:
            raise ValueError("unsupported condition group operator", self)

    def selectedIndex(self, report, conditions, groupByID):
        if self is RuleConditionGroupOperator.all:
            index = report.index
        elif self is RuleConditionGroupOperator.any:
            index = pd.Int64Index([])
        else:
            raise ValueError("unsupported condition group operator", self)

        undeterminedData = report

        for condition in conditions:
            index = self.combineIndicies(index, condition.selectedIndex(undeterminedData, groupByID=groupByID))
            undeterminedData = self.undeterminedData(report, selectedIndex=index)

        return index

class RuleConditionGroup(IOMap):
  def __init__(self,
               conditions=None,
               subgroups=None,
               operator=None):
    self.conditions = conditions
    self.subgroups = subgroups
    self.operator = operator
    self.prepare_maps()

  @classmethod
  def groupWithID(cls, conditionGroupsCollection, id):
    data = conditionGroupsCollection.find_one({"_id": ObjectId(id)})

    conditions = [
      RuleCondition(kpi=RuleKPI(c["metric"]),
                    operator=RuleConditionalOperator(c["operator"]),
                    comparisonValue=c["metricValue"])
      for c in data["conditions"]
    ]

    subgroups = [RuleConditionGroup.groupWithID(conditionGroupsCollection=conditionGroupsCollection, id=str(i)) for i in data["subgroups"]]

    group = cls(conditions=conditions,
                subgroups=subgroups,
                operator=RuleConditionGroupOperator(data["operator"]))

    return group

  def selectedIndex(self, report, groupByID):
    index = self.operator.selectedIndex(report, conditions=self.conditions, groupByID=groupByID)

    undeterminedData = self.operator.undeterminedData(report, selectedIndex=index)

    for subgroup in self.subgroups:
      index = self.operator.combineIndicies(index, subgroup.selectedIndex(undeterminedData, groupByID=groupByID))
      undeterminedData = self.operator.undeterminedData(undeterminedData, selectedIndex=index)

    return index

  def filterData(self, report, groupByID="keywordId"):
    if report.empty: return

    index = self.selectedIndex(report, groupByID=groupByID)
    dropIndex = report.index.difference(index)
    report.drop(dropIndex, inplace=True)

  #----------
  # IOMap
  #----------
  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'condition_group'

  @classmethod
  def get_run_keys(cls) -> List[str]:
    return ['selected_indices']

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return ['selected_index']

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: AllMap,
        IOMapKey.construct.value: {
          'maps': 'construct.conditions',
        },
        IOMapKey.input.value: {
          'each_input.report': 'run.report',
        },
        IOMapKey.output.value: {
          'results': 'run.condition_indices',
        },
      }
    ]

  def run(self, report: pd.DataFrame, group_by_id: str) -> Dict[str, any]:
    super().run(
      report=report,
      group_by_id=group_by_id
    )
    return self.populated_output