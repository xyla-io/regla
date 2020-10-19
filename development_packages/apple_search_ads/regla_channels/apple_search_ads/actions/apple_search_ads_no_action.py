import pdb
from .apple_search_ads_actions import SearchAdsAction
from regla import RuleActionResult, RuleActionLog, RuleActionTargetType

class SearchAdsNoAction(SearchAdsAction):
    def adjust(self, api, campaign, report, dryRun=False):
        if report.empty:
            return RuleActionResult(
                report=report,
                dryRun=dryRun
            )

        keywordData = report.copy()
        groupedKeywordData = keywordData.copy().groupby("adGroupId")
        keywordGroups = groupedKeywordData.groups
        adjustmentLogs = []
        for adGroupId in keywordGroups:
            adGroups = [g for g in campaign.ad_groups if g._id == str(adGroupId)]
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
                
                log = RuleActionLog(
                    targetID=int(keywordId),
                    targetType=RuleActionTargetType.keyword,
                    targetDescription="'{text}' in {adgroup}".format(text=keyword.text, adgroup=adGroup.name),
                    actionDescription="No action taken"
                )
                adjustmentLogs.append(log)

        result = RuleActionResult(
            apiResponse="No API Response",
            report=keywordData,
            dryRun=dryRun,
            errors=[],
            logs=adjustmentLogs
        )
        return result