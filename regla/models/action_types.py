from enum import Enum

class RuleActionType(Enum):
  increaseBid = 'inc_bid'
  decreaseBid = 'dec_bid'
  increaseCPAGoal = 'inc_cpa_goal'
  decreaseCPAGoal = 'dec_cpa_goal'
  increaseCPAGoalCampaign = 'inc_cpa_goal_campaign'
  decreaseCPAGoalCampaign = 'dec_cpa_goal_campaign'
  pauseKeyword = 'pause_keyword'
  pauseCampaign = 'pause_campaign'
  noAction = 'no_action'
  increase_camapign_budget = 'increase_campaign_budget'
  decrease_campaign_budget = 'decrease_campaign_budget'
