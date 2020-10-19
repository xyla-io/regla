from .apple_search_ads_actions import SearchAdsAction
from heathcliff.models import Keyword
from regla import RuleActionResult, RuleActionLog, RuleActionTargetType

import pdb

class SearchAdsBidAction(SearchAdsAction):
    def adjust(self, api, campaign, report, dryRun=False):
        bidManager = BidManager(campaign=campaign, keywordData=report, adjustmentMultiplier=self.adjustmentValue, limit=self.adjustmentLimit)
        result = bidManager.adjustBids(dryRun=dryRun)

        return result

class BidManager(object):
    def __init__(self,
                 campaign=None,
                 keywordData=None,
                 adjustmentMultiplier=None,
                 limit=None):
        self.campaign = campaign
        self.keywordData = keywordData
        self.adjustmentMultiplier = adjustmentMultiplier
        self.limit = limit

    def adjustBids(self, dryRun=False):
        if self.keywordData.empty:
            return RuleActionResult(
                report=self.keywordData,
                dryRun=dryRun
            )

        keywordData = self.keywordData.copy()
        keywordData["originalBid"] = ""
        keywordData["adjustedBid"] = ""
        groupedKeywordData = keywordData.copy().groupby("adGroupId")
        keywordGroups = groupedKeywordData.groups
        adjustedKeywords = []
        adjustmentLogs = []
        budgetAmount = float(self.campaign.budget_amount["amount"])
        for adGroupId in keywordGroups:
            adGroups = [g for g in self.campaign.ad_groups if g._id == str(adGroupId)]
            adGroup = next(iter(adGroups), None)
            if not adGroup:
                continue

            adGroupData = groupedKeywordData.get_group(adGroupId)
            keywordIds = adGroupData.keywordId.unique()
            for keywordId in keywordIds:
                if keywordData[keywordData.keywordId == keywordId].empty:
                    continue

                keywords = [k for k in adGroup.keywords if k._id == str(keywordId)]
                keyword = next(iter(keywords), None)
                if not keyword:
                    continue

                originalAmount = float(keyword.bid_amount["amount"])
                amount = originalAmount * self.adjustmentMultiplier

                keywordData.loc[keywordData.keywordId == keywordId, "originalBid"] = "{0:.2f}".format(originalAmount)

                if self.adjustmentMultiplier >= 1.0:
                    if originalAmount >= self.limit:
                        keywordData.drop(keywordData[keywordData.keywordId == keywordId].index, inplace=True)
                        continue
                    if amount > self.limit:
                        amount = self.limit
                    # Do not exceed campaign budget
                    if originalAmount >= budgetAmount:
                        keywordData.drop(keywordData[keywordData.keywordId == keywordId].index, inplace=True)
                        continue
                    if amount > budgetAmount:
                        amount = budgetAmount
                else:
                    if originalAmount <= self.limit:
                        keywordData.drop(keywordData[keywordData.keywordId == keywordId].index, inplace=True)
                        continue
                    if amount < self.limit:
                        amount = self.limit

                log = RuleActionLog(
                    targetID=int(keywordId),
                    targetType=RuleActionTargetType.keyword,
                    targetDescription="'{text}' in {adgroup}".format(text=keyword.text, adgroup=adGroup.name),
                    actionDescription="Adjusted bid from {original} to {adjusted} ({currency})".format(original=keyword.bid_amount["amount"], adjusted="{0:.2f}".format(amount), currency=keyword.bid_amount["currency"])
                )
                adjustmentLogs.append(log)

                keywordData.loc[keywordData.keywordId == keywordId, 'adjustedBid'] = "{0:.2f}".format(amount)

                keyword.bid_amount["amount"] = "{0:.2f}".format(amount)
                adjustedKeywords.append(keyword)

        if not adjustedKeywords or dryRun:
            return RuleActionResult(
                report=keywordData,
                logs=adjustmentLogs,
                dryRun=dryRun
            )

        apiResponses = Keyword.update_keywords(adjustedKeywords, self.campaign._id)

        return RuleActionResult(
            apiResponse=apiResponses,
            report=keywordData,
            errors=[a["error"] for a in apiResponses if a["error"]],
            logs=adjustmentLogs,
            dryRun=dryRun
        )