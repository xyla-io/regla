import unittest
import pdb
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_index_equal
from ..models.condition_models import RuleKPI, RuleConditionalOperator

class Test_kpi(unittest.TestCase):
    def test_spend(self):
        """
        Test adding data for spend
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 2., 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.spend
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "totalSpend": pd.Series([4., 2., 4.], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "totalSpend"]], pd.DataFrame(d))

    def test_impressions(self):
        """
        Test adding data for impressions
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "impressions": pd.Series([1., 2., 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.impressions
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "totalImpressions": pd.Series([4., 2., 4.], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "totalImpressions"]], pd.DataFrame(d))

    def test_taps(self):
        """
        Test adding data for taps
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "taps": pd.Series([1., 2., 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.taps
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "totalTaps": pd.Series([4., 2., 4.], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "totalTaps"]], pd.DataFrame(d))

    def test_conversions(self):
        """
        Test adding data for conversions
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "installs": pd.Series([1., 2., 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.conversions
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "totalConversions": pd.Series([4., 2., 4.], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "totalConversions"]], pd.DataFrame(d))

    def test_ttr(self):
        """
        Test adding data for TTR
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "impressions": pd.Series([1., 0., 3.]),
            "taps": pd.Series([1., 4., 2.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.ttr
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "reavgTTR": pd.Series([0.75, np.nan, 0.75], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "reavgTTR"]], pd.DataFrame(d))

    def test_conversion_rate(self):
        """
        Test adding data for conversion rate
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "taps": pd.Series([1., 0., 3.]),
            "installs": pd.Series([1., 4., 2.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.conversionRate
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "reavgConversionRate": pd.Series([0.75, np.nan, 0.75], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "reavgConversionRate"]], pd.DataFrame(d))

    def test_cpt(self):
        """
        Test adding data for CPT
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 4., 2.]),
            "taps": pd.Series([1., 0, 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.cpt
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "reavgCPT": pd.Series([0.75, np.nan, 0.75], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "reavgCPT"]], pd.DataFrame(d))

    def test_cpa(self):
        """
        Test adding data for CPA
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 4., 2.]),
            "installs": pd.Series([1., 0, 3.]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.cpa
        kpi.addRequiredColumns(df, groupByID="keywordId")

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "reavgCPA": pd.Series([0.75, np.nan, 0.75], index=dataIndex),
        }
        assert_frame_equal(df.loc[:, ["keywordId", "reavgCPA"]], pd.DataFrame(d))

class Test_kpi_selection(unittest.TestCase):
    def test_spend(self):
        """
        Test selection by spend
        """
        d = {
            "totalSpend": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.spend
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_impressions(self):
        """
        Test selection by impressions
        """
        d = {
            "totalImpressions": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.impressions
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_taps(self):
        """
        Test selection by taps
        """
        d = {
            "totalTaps": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.taps
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_conversions(self):
        """
        Test selection by conversions
        """
        d = {
            "totalConversions": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.conversions
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_ttr(self):
        """
        Test selection by TTR
        """
        d = {
            "reavgTTR": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.ttr
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_conversion_rate(self):
        """
        Test selection by conversion rate
        """
        d = {
            "reavgConversionRate": pd.Series([3., 1., 2., 1., np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.conversionRate
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

    def test_cpt(self):
        """
        Test selection by CPT
        """
        d = {
            "totalSpend": pd.Series([3., 1., 2., 1., 2., 6.]),
            "reavgCPT": pd.Series([3., 1., 2., 1., np.nan, np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.cpt
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

        index = kpi.selectedIndex(df, RuleConditionalOperator.greater, 2.)
        assert_index_equal(index, pd.Int64Index([0, 5]))

    def test_cpa(self):
        """
        Test selection by CPA
        """
        d = {
            "totalSpend": pd.Series([3., 1., 2., 1., 2., 6.]),
            "reavgCPA": pd.Series([3., 1., 2., 1., np.nan, np.nan]),
             }
        df = pd.DataFrame(d)

        kpi = RuleKPI.cpa
        index = kpi.selectedIndex(df, RuleConditionalOperator.less, 3.)
        assert_index_equal(index, pd.Int64Index([1, 2, 3]))

        index = kpi.selectedIndex(df, RuleConditionalOperator.greater, 2.)
        assert_index_equal(index, pd.Int64Index([0, 5]))


if __name__ == '__main__':
    unittest.main()

