import pandas as pd

from time import sleep
from datetime import datetime
from moda import log
from regla import RuleReporter, RuleReportType, RuleReportGranularity

class SearchAdsReporter(RuleReporter):
  def _get_report_function(self, api: any) -> any:
    if self.reportType is RuleReportType.keyword:
      return api.get_campaign_keywords_report
    elif self.reportType is RuleReportType.adGroup:
      return api.get_campaign_adgroups_report
    elif self.reportType is RuleReportType.searchTerm:
      return api.get_campaign_searchterms_report
    else:
      raise ValueError('Unsupported search ads report type', self.reportType)

  def _getRawReport(self, startDate, endDate, granularity, api, campaign, adGroupIDs):
    tries = 3
    while True:
      try:
        granularity = RuleReportGranularity(granularity)
        selector = {
          "orderBy": [
            {
              "field": "impressions",
              "sortOrder": "DESCENDING"
            }
          ],
          "pagination": {
            "offset": 0, "limit": 1000
          }
        }
        if adGroupIDs is not None:
          selector["conditions"] = [{
            "field": "adGroupId",
            "operator": "IN",
            "values": adGroupIDs,
          }]

        date_format = "%Y-%m-%d"
        start_date_string = startDate.strftime(date_format)
        end_date_string = endDate.strftime(date_format)

        frames = self._get_report_function(api)(
          campaign=campaign,
          start_time=start_date_string,
          end_time=end_date_string,
          granularity=granularity.value,
          return_records_with_no_metrics=False,
          return_row_totals=False,
          selector=selector,
        )

        frames["campaignName"] = campaign.name
        frames["orgId"] = campaign._org_id
        frames["orgName"] = api.name

        report = pd.concat([frames], sort=True)
        if report.empty: return report

        report.date = report.date.apply(lambda d: datetime.strptime(d, granularity.dateFormatString))
        return report
      except (SystemExit, KeyboardInterrupt):
        raise
      except Exception as e:
        tries -= 1
        if not tries:
          raise
        log.log(f'Apple Search Ads report exception {repr(e)}\n\nWill retry after 30 seconds...')
        sleep(30)