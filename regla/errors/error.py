class RuleError(Exception):
  pass

class RuleActionError(RuleError):
  pass

class RuleActionMissingTargetError(RuleActionError):
  def __init__(self, target_id: str):
    super().__init__(f'Cannot locate action target (ID {target_id})')

class RuleActionEntityError(RuleActionError):
  def __init__(self, target_id: str, error: Exception, traceback: str):
    super().__init__(f'Action entity error for target ID {target_id} error:\n{repr(error)}\ntraceback:\n{traceback}')