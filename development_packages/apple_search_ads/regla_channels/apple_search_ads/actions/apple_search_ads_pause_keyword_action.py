from heathcliff.models import Keyword
from .apple_search_ads_actions import SearchAdsAction
from regla import RuleActionResult, RuleActionLog, RuleActionTargetType


class SearchAdsPauseKeywordAction(SearchAdsAction):
    def adjust(self, api, campaign, report, dryRun=False):
        pauseKeywordManager = PauseKeywordManager(campaign=campaign, keywordData=report)
        result = pauseKeywordManager.pauseKeywords(dryRun=dryRun)

        return result

class PauseKeywordManager(object):
    def __init__(self,
                 campaign=None,
                 keywordData=None):
        self.campaign = campaign
        self.keywordData = keywordData

    def pauseKeywords(self, dryRun=False):
        if self.keywordData.empty:
            return RuleActionResult(
                report=self.keywordData,
                dryRun=dryRun
            )

        keywordData = self.keywordData.copy()
        groupedKeywordData = keywordData.copy().groupby("adGroupId")
        keywordGroups = groupedKeywordData.groups
        adjustedKeywords = []
        adjustmentLogs = []

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
                    keywordData.drop(keywordData[keywordData.keywordId == keywordId].index, inplace=True)
                    continue

                keyword.pause()
                adjustedKeywords.append(keyword)

                log = RuleActionLog(
                    targetID=int(keywordId),
                    targetType=RuleActionTargetType.keyword,
                    targetDescription="'{keyword}' in {adgroup}".format(keyword=keyword.text, adgroup=adGroup.name),
                    actionDescription="changed status to {status}".format(status=keyword.status)
                )
                adjustmentLogs.append(log)

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