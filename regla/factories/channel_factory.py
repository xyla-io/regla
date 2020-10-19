import importlib

from ..models.channel_models import Channel
from typing import Dict

def channel_factory(channel_identifier: str, options: Dict[str, any]={}) -> Channel:
  channel_module = importlib.import_module(f'regla_channels.{channel_identifier}')
  return channel_module.Channel(options=options)
