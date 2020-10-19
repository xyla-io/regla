from .apple_search_ads_actions import SearchAdsAction
from regla import RuleActionResult, RuleActionLog, RuleActionTargetType
import pdb

class SearchAdsCPAGoalAction(SearchAdsAction):
    def adjust(self, api, campaign, report, dryRun=False):
        goalManager = CPAGoalManager(
            campaign=campaign,
            adgroupData=report,
            adjustmentMultiplier=self.adjustmentValue,
            limit=self.adjustmentLimit
        )
        result = goalManager.adjustGoal(dryRun=dryRun)
        return result

class CPAGoalManager(object):
    def __init__(self,
                 campaign=None,
                 adgroupData=None,
                 adjustmentMultiplier=None,
                 limit=None):
        self.campaign = campaign
        self.adgroupData = adgroupData
        self.adjustmentMultiplier = adjustmentMultiplier
        self.limit = limit

    def adjustGoal(self, dryRun=False):
        if self.adgroupData.empty:
            return RuleActionResult(
                report=self.adgroupData,
                dryRun=dryRun
            )

        adgroupData = self.adgroupData.copy()
        adgroupData["originalGoal"] = ""
        adgroupData["adjustedGoal"] = ""
        groupedAdgroupData = adgroupData.copy().groupby("adGroupId")
        adgroupGroups = groupedAdgroupData.groups

        apiResponses = []

        adjustmentLogs = []
        for adGroupId in adgroupGroups:
            if adgroupData[adgroupData.adGroupId == adGroupId].empty:
                continue

            adGroups = [g for g in self.campaign.ad_groups if g._id == str(adGroupId)]
            adGroup = next(iter(adGroups), None)
            if not adGroup or not adGroup.cpa_goal:
                continue

            originalGoal = float(adGroup.cpa_goal["amount"])
            amount = originalGoal * self.adjustmentMultiplier

            adgroupData.loc[adgroupData.adGroupId == adGroupId, "originalGoal"] = "{0:.2f}".format(originalGoal)

            if self.adjustmentMultiplier >= 1.0:
                if originalGoal >= self.limit:
                    adgroupData.drop(adgroupData[adgroupData.adGroupId == adGroupId].index, inplace=True)
                    continue
                if amount > self.limit:
                    amount = self.limit
            else:
                if originalGoal <= self.limit:
                    adgroupData.drop(adgroupData[adgroupData.adGroupId == adGroupId].index, inplace=True)
                    continue
                if amount < self.limit:
                    amount = self.limit

            adgroupData.loc[adgroupData.adGroupId == adGroupId, "adjustedGoal"] = "{0:.2f}".format(amount)

            log = RuleActionLog(
                targetID=int(adGroupId),
                targetType=RuleActionTargetType.adgroup,
                targetDescription="'{adgroup}' in {campaign}".format(adgroup=adGroup.name, campaign=self.campaign.name),
                actionDescription="Adjusted CPA goal from {originalGoal} to {adjustedGoal} ({currency})".format(originalGoal=adGroup.cpa_goal["amount"],
                                                                         adjustedGoal="{0:.2f}".format(amount),
                                                                                       currency=adGroup.cpa_goal["currency"])
            )
            adjustmentLogs.append(log)
            adGroup.cpa_goal["amount"] = "{0:.2f}".format(amount)

            if not dryRun:
                response = adGroup.update_adgroup()
                apiResponses.append(response)

        if not apiResponses or dryRun:
            return RuleActionResult(
                report=adgroupData,
                logs=adjustmentLogs,
                dryRun=dryRun
            )

        errors = [a["error"] for a in apiResponses if a["error"]]
        return RuleActionResult(
            apiResponse=apiResponses,
            report=adgroupData,
            errors=errors,
            logs=adjustmentLogs,
            dryRun=dryRun
        )
