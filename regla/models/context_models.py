from __future__ import annotations
from enum import Enum
from typing import Dict

class RuleContext(Enum):
  now = 'now'
  rule = 'rule'
  rule_options = 'rule_options'
  channel = 'channel'
  history_collection = 'history_collection'
  rule_collection = 'rule_collection'
  monitor_collection = 'monitor_collection'
  channel_context = 'channel_context'

class RuleContextOption:
  @classmethod
  def get_defaults(cls) -> Dict[str, any]:
    return {
      o.value: o.default
      for o in cls
    }
  
  @property
  def default(self) -> any:
    raise NotImplementedError()

class RuleOption(RuleContextOption, Enum):
  dynamic_window = 'dynamic_window'
  use_dry_run_history = 'use_dry_run_history'
  
  @property
  def default(self) -> any:
    if self is RuleOption.dynamic_window:
      return True
    elif self is RuleOption.use_dry_run_history:
      return False
    else:
      raise ValueError('Unsupported rule option', self)
