from src.experiment.OrangeTableResourceColumnGenerator import OrangeTableResourceColumnGenerator
from src.config import config


class ResourceFlattner(object):
    def __init__(self, event_normalizer, resource_field_values):
        self.event_normalizer = event_normalizer
        self.resource_field_values = resource_field_values

    def encode_resource_to_map(self, resources):
        result = {}
        for type, ids in self.resource_field_values.items():
            result[type] = 'None'
        if resources:
            for resource in resources:
                if resource['resource_type'] == 'None':
                    continue
                key = 'resource_%s' % resource['resource_type']
                result[key] = resource['resource_id']
        return result

    def normalized_user_op_resource_from_event_encoded_resources(self, event):
        normalized_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
        return self.flatten_resources(normalized_event)

    def flatten_resources(self, event):
        encoded_resources = self.encode_resource_to_map(event['resources'])
        event.pop('resources')
        event.update(encoded_resources)
        return event