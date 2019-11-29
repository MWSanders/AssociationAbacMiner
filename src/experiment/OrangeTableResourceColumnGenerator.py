from Orange.data import DiscreteVariable

from src.config import config

client = config.client
db = client.calp
events_collection = db.events


class OrangeTableResourceColumnGenerator(object):
    def __init__(self, field_values):
        self.field_values = field_values
        self.columns = []
        for key, values in self.field_values.items():
            values.add('None')
            column = DiscreteVariable('%s' % (key), values=list(values))
            self.columns.append(column)
        self.columns.sort(key=lambda x: x.name, reverse=True)

    def get_table_columns(self):
        return self.columns

    def encode_resource_to_map(self, resources):
        result = {}
        for type, ids in self.field_values.items():
            result[type] = 'None'
        if resources:
            for resource in resources:
                if resource['resource_type'] == 'None':
                    continue
                key = 'resource_%s' % resource['resource_type']
                result[key] = resource['resource_id']
        return result


if __name__ == "__main__":
    query = config.mongo_source_query
    resource_encoder = OrangeTableResourceColumnGenerator(query)
    test_empty_list = []
    test_none = [{'resource_id': 'None',
                      'resource_type': 'None'}]
    columns = resource_encoder.get_table_columns()
    encoded_empty = resource_encoder.encode_resource_to_map(test_empty_list)
    encoded_none = resource_encoder.encode_resource_to_map(test_none)
