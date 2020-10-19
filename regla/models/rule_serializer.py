import json

class RuleSerializer(json.JSONEncoder):
  def default(self, o):
    if hasattr(o, 'serialize_result'):
      return o.serialize_result()
    return json.JSONEncoder.default(self, o)
