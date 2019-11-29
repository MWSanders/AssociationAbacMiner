import sys
import os


class RuleEvaluator(object):
    def __init__(self, service_ops_types):
        self.service_ops_types = service_ops_types
        pass

    def rule_allows_event(self, event, constraint_map):
        for constraint_key, constraint_value in constraint_map.items():
            if constraint_key in event:
                event_value = event[constraint_key]
                if isinstance(constraint_value, list):
                    first_value = constraint_value[0]
                    event_value = self.coerce_type_and_test(event_value, first_value)
                    if event_value not in constraint_value:
                        return False
                else:
                    event_value = self.coerce_type_and_test(event_value, constraint_value)
                    event_allows_none_type = (constraint_key == 'resource_op_eventName' and 'None' in  self.service_ops_types['iam'][event['resource_op_eventName']])
                    if event_value == 'None' and event_allows_none_type:
                        continue
                    if event_value != constraint_value:
                        print('Rule required %s=%s, found %s=%s' % (constraint_key, constraint_value, constraint_key, event_value))
                        return False
            else:
                return False
        # Verify all resource statements in constrain_map are valid.  Resource statments must be validated
        return True

    def coerce_type_and_test(self, source_value, target_value):
        if isinstance(target_value, str) and isinstance(source_value, int):
            source_value = str(source_value)
        if type(source_value) != type(target_value):
            print("ERROR: POLICY & EVENT VALUES OF DIFFERENT TYPE")
            sys.exit(1)
        return source_value
