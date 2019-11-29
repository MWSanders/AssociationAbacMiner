import json
from src.model import RuleUtils

class RuleEval:
    def __init__(self):
        self.constraints = None
        self.constraints_map = {}
        self.all_log_entries = 0
        self.unique_log_entries = 0
        self.overassignment_rate_all = 0
        self.coverage_rate = 0
        self.under_assignments = 0
        self.overassignment_rate_unique = 0
        self.coverage_rate_unique = 0
        self.sort_metric_value = 0
        self.overassignment_total = 0
        self.allowed_events_count = 0
        self.allowed_ops = 0
        self.allowed_users = 0
        self.allowed_resources = 0
        self.scores = {}
        self.q = {}

    def __str__(self):
        obj_as_dict = self.__dict__
        del obj_as_dict['query']
        return json.dumps(obj_as_dict, sort_keys=True, cls=SetEncoder)

    def set_query(self, query):
        self.q = query

    def set_constrants(self, constraints):
        self.constraints = constraints
        for constraint in constraints:
            key = constraint.split('=')[0]
            value = constraint.split('=')[1]
            RuleUtils.addMulti(self.constraints_map, key, value)

    def to_dict(self):
        self.constraints = list(self.constraints)
        for key in self.constraints_map:
            self.constraints_map[key] = list(self.constraints_map[key])
        return self.__dict__


class SetEncoder(json.JSONEncoder):
   def default(self, obj):
      if isinstance(obj, set):
          return list(obj)
      if isinstance(obj, RuleEval):
          return obj.__dict__
      return json.JSONEncoder.default(self, obj)