import unittest
import pdb
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_index_equal

from ..models.condition_models import RuleKPI, RuleCondition, RuleConditionalOperator, RuleConditionGroup, RuleConditionGroupOperator


class Test_conditional_operator(unittest.TestCase):
    def setUp(self):
        """
        Create sample data
        """
        d = {
            "datum": pd.Series([3., 2., 1., np.nan]),
            "criterion": pd.Series([np.nan, 1., 2., 3.]),
             }
        self.df = pd.DataFrame(d)

    def test_greater(self):
        """
        Test filtering by greater than
        """
        operator = RuleConditionalOperator.greater
        index = operator.selectedIndex(self.df, "criterion", 2.)
        self.assertEqual(index, pd.Int64Index([3]))

    def test_less(self):
        """
        Test filtering by greater than
        """
        operator = RuleConditionalOperator.less
        index = operator.selectedIndex(self.df, "criterion", 2.)
        self.assertEqual(index, pd.Int64Index([1]))


    def test_greater_than_or_equal(self):
        """
        Test filtering by greater than
        """
        operator = RuleConditionalOperator.greaterThanOrEqual
        index = operator.selectedIndex(self.df, "criterion", 2.)
        assert_index_equal(index, pd.Int64Index([2, 3]))

    def test_less_than_or_equal(self):
        """
        Test filtering by greater than
        """
        operator = RuleConditionalOperator.lessThanOrEqual
        index = operator.selectedIndex(self.df, "criterion", 2.)
        assert_index_equal(index, pd.Int64Index([1, 2]))

    def test_equal(self):
        """
        Test filtering by greater than
        """
        operator = RuleConditionalOperator.equal
        index = operator.selectedIndex(self.df, "criterion", 2.)
        assert_index_equal(index, pd.Int64Index([2]))

class Test_condiition(unittest.TestCase):
    """
    Test module for the search ads condition classes
    """

    def test_spend(self):
        """
        Test filtering by total spend
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 3., 3.]),
             }
        df = pd.DataFrame(d)

        condition = RuleCondition(kpi=RuleKPI("totalSpend"),
                                       operator=RuleConditionalOperator("greater"),
                                       comparisonValue=3.)

        index = condition.selectedIndex(df, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([0, 2]))

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "localSpend": pd.Series([1., 3., 3.], index=dataIndex),
            "totalSpend": pd.Series([4., 3., 4.], index=dataIndex),
        }
        assert_frame_equal(df.sort_index(axis=1), pd.DataFrame(d).sort_index(axis=1))

    def test_cpt(self):
        """
        Test filtering by total CPT
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 3., 3.]),
            "taps": pd.Series([0, 0, 2.]),
             }
        df = pd.DataFrame(d)

        condition = RuleCondition(kpi=RuleKPI("reavgCPT"),
                                       operator=RuleConditionalOperator("less"),
                                       comparisonValue=3.)

        index = condition.selectedIndex(df, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([0, 2]))

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "localSpend": pd.Series([1., 3., 3.], index=dataIndex),
            "taps": pd.Series([0, 0, 2.], index=dataIndex),
            "totalSpend": pd.Series([4., 3., 4.], index=dataIndex),
            "reavgCPT": pd.Series([2., np.nan, 2.], index=dataIndex),
            "totalTaps": pd.Series([2., 0, 2.], index=dataIndex),
        }
        assert_frame_equal(df.sort_index(axis=1), pd.DataFrame(d).sort_index(axis=1))

    def test_cpa(self):
        """
        Test filtering by total CPA
        """
        d = {
            "keywordId": pd.Series([1, 2, 1]),
            "localSpend": pd.Series([1., 3., 3.]),
            "installs": pd.Series([0, 1., 2.]),
             }
        df = pd.DataFrame(d)

        condition = RuleCondition(kpi=RuleKPI("reavgCPA"),
                                       operator=RuleConditionalOperator("less"),
                                       comparisonValue=3.)

        index = condition.selectedIndex(df, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([0, 2]))

        dataIndex = [0, 1, 2]
        d = {
            "keywordId": pd.Series([1, 2, 1], index=dataIndex),
            "localSpend": pd.Series([1., 3., 3.], index=dataIndex),
            "installs": pd.Series([0, 1., 2.], index=dataIndex),
            "totalSpend": pd.Series([4., 3., 4.], index=dataIndex),
            "reavgCPA": pd.Series([2., 3., 2.], index=dataIndex),
            "totalConversions": pd.Series([2., 1., 2.], index=dataIndex),
        }
        assert_frame_equal(df.sort_index(axis=1), pd.DataFrame(d).sort_index(axis=1))


class Test_condition_group_operator(unittest.TestCase):
    def setUp(self):
        """
        Create sample data
        """
        d = {
            "keywordId": pd.Series([1, 2, 3]),
            "localSpend": pd.Series([1., 2., 3.]),
             }
        self.df = pd.DataFrame(d)

    def test_all(self):
        """
        Test filtering by all
        """
        operator = RuleConditionGroupOperator.all
        conditions = [
            RuleCondition(kpi=RuleKPI("totalSpend"),
                               operator=RuleConditionalOperator("greater"),
                               comparisonValue=1.),
            RuleCondition(kpi=RuleKPI("totalSpend"),
                               operator=RuleConditionalOperator("less"),
                               comparisonValue=3.),
        ]

        index = operator.selectedIndex(self.df, conditions=conditions, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([1]))

    def test_any(self):
        """
        Test filtering by any
        """
        operator = RuleConditionGroupOperator.any
        conditions = [
            RuleCondition(kpi=RuleKPI("totalSpend"),
                               operator=RuleConditionalOperator("greater"),
                               comparisonValue=2.),
            RuleCondition(kpi=RuleKPI("totalSpend"),
                               operator=RuleConditionalOperator("less"),
                               comparisonValue=2.),
        ]

        index = operator.selectedIndex(self.df, conditions=conditions, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([0, 2]))


class Test_condition_group(unittest.TestCase):
    def setUp(self):
        """
        Create sample data
        """
        d = {
            "keywordId": pd.Series([1, 2, 3]),
            "localSpend": pd.Series([1., 2., 3.]),
             }
        self.df = pd.DataFrame(d)

    def test_all(self):
        """
        Test filtering by all
        """
        subgroup = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                       operator=RuleConditionalOperator("greater"),
                                                                       comparisonValue=1.)],
                                        subgroups=[],
                                        operator=RuleConditionGroupOperator.all)

        group = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                       operator=RuleConditionalOperator("less"),
                                                                       comparisonValue=3.)],
                                        subgroups=[subgroup],
                                        operator=RuleConditionGroupOperator.all)

        index = group.selectedIndex(self.df, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([1]))

    def test_any(self):
        """
        Test filtering by any
        """
        subgroup = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                          operator=RuleConditionalOperator(
                                                                              "greater"),
                                                                          comparisonValue=2.)],
                                           subgroups=[],
                                           operator=RuleConditionGroupOperator.any)

        group = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                       operator=RuleConditionalOperator(
                                                                           "less"),
                                                                       comparisonValue=2.)],
                                        subgroups=[subgroup],
                                        operator=RuleConditionGroupOperator.any)

        index = group.selectedIndex(self.df, groupByID="keywordId")
        assert_index_equal(index, pd.Int64Index([0, 2]))

    def test_filter(self):
        """
        Test filtering data
        """
        subgroup = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                       operator=RuleConditionalOperator("greater"),
                                                                       comparisonValue=1.)],
                                        subgroups=[],
                                        operator=RuleConditionGroupOperator.all)

        group = RuleConditionGroup(conditions=[RuleCondition(kpi=RuleKPI("totalSpend"),
                                                                       operator=RuleConditionalOperator("less"),
                                                                       comparisonValue=3.)],
                                        subgroups=[subgroup],
                                        operator=RuleConditionGroupOperator.all)

        group.filterData(self.df, groupByID="keywordId")

        dataIndex = [1]
        d = {
            "keywordId": pd.Series([2], index=dataIndex),
            "localSpend": pd.Series([2.], index=dataIndex),
            "totalSpend": pd.Series([2.], index=dataIndex),
        }
        assert_frame_equal(self.df.sort_index(axis=1), pd.DataFrame(d).sort_index(axis=1))

if __name__ == '__main__':
    unittest.main()

