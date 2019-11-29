import traceback

import dateutil.parser
import six
from Orange.data import *
from elasticsearch import helpers
from collections import Counter
from src.config import config
from src.model import RuleUtils

client = config.client
events_collection = config.events_collection
jobs_collection = config.jobs_collection
service_op_resource_collection = config.service_op_resource_collection
es = config.es

def _construct_key(previous_key, separator, new_key):
    """
    Returns the new_key if no previous key exists, otherwise concatenates
    previous key, separator, and new_key
    :param previous_key:
    :param separator:
    :param new_key:
    :return: a string if previous_key exists and simply passes through the
    new_key otherwise
    """
    if previous_key:
        return u"{}{}{}".format(previous_key, separator, new_key)
    else:
        return new_key

bad_keys = set()
def flatten(nested_dict, separator="_", root_keys_to_ignore=set()):
    """
    Flattens a dictionary with nested structure to a dictionary with no
    hierarchy
    Consider ignoring keys that you are not interested in to prevent
    unnecessary processing
    This is specially true for very deep objects
    :param nested_dict: dictionary we want to flatten
    :param separator: string to separate dictionary keys by
    :param root_keys_to_ignore: set of root keys to ignore from flattening
    :return: flattened dictionary
    """
    assert isinstance(nested_dict, dict), "flatten requires a dictionary input"
    assert isinstance(separator, six.string_types), "separator must be string"

    # This global dictionary stores the flattened keys and values and is
    # ultimately returned
    flattened_dict = dict()

    def _flatten(object_, key):
        """
        For dict, list and set objects_ calls itself on the elements and for
        other types assigns the object_ to
        the corresponding key in the global flattened_dict
        :param object_: object to flatten
        :param key: carries the concatenated key for the object_
        :return: None
        """
        # Empty object can't be iterated, take as is
        if not object_:
            flattened_dict[key] = object_
        # These object types support iteration
        elif isinstance(object_, dict):
            for object_key in object_:
                if not (not key and object_key in root_keys_to_ignore):
                    _flatten(object_[object_key], _construct_key(key,
                                                                 separator,
                                                                 object_key))
        elif isinstance(object_, list) or isinstance(object_, set):
            return
            # for index, item in enumerate(object_):
            #     _flatten(item, _construct_key(key, separator, index))
        # Anything left take as is
        else:
            flattened_dict[key] = str(object_).replace('\t', ' ')
            if len(str(object_).replace('\t', ' ')) > 256:
                bad_keys.add(key)

    _flatten(nested_dict, None)
    return flattened_dict


def createInstance(domain, values_dict, resource_encoder, prune_null_resources):
    data = []
    if resource_encoder:
        encoded_resources = resource_encoder.encode_resource_to_map(values_dict['resources'])
        values_dict.update(encoded_resources)
    for attribute in domain.attributes:
        if prune_null_resources and attribute.name.startswith('requestParameters_'):
            data.append(None)
        elif attribute.name in values_dict:
            data.append(str(values_dict[attribute.name]))
        else:
            data.append(None)
    instance = Instance(domain, data)
    return instance


def shouldFilterEvent(event):
    # if event['eventName'].startswith("Describe") or event['eventName'].startswith("List") or event['eventName'].startswith("Discover") \
    #         or event['eventName'].startswith("AssumeRole") or event['eventName'].startswith("GetPipeline") or 'errorCode' in event:
    #     return True
    return False


def build_orange_table_from_es_logs(mongo_query, valid_keys=None, prune_null_resources=True, all_logs_index='flat-all-log-entries', unique_logs_index='flat-unique-log-entries'):
    field_values = {}
    single_value_columns = set() #TODO return values that are always true, add them to Rules
    records = 0
    key_value_counter = Counter()
    paginator = helpers.scan(es, query={"query": {"match_all": {}}}, index=unique_logs_index, doc_type='doc')
    for hit in paginator:
        records += 1
        # if records % 1000 == 0:
        #     print('Records : ' + str(records))
        for key, value in hit['_source'].items():
            if key == '_id' or (valid_keys and key not in valid_keys):
                continue
            RuleUtils.addMulti(field_values, key, value)
            key_value_counter.update(['%s=%s' % (key, value)])

    for k, v in dict(key_value_counter).items():
        if v == records:
            single_value_columns.add(k)  # ignore fields that always have the same value
            field_name = k.split('=')[0]
            field_values.pop(field_name)

    orange_columns = []
    for key, value in field_values.items():
        # if len(value) == 1 and records > 1:
        #     single_value_columns.add('%s=%s' % (key, value.pop()))#ignore fields that always have the same value
        #     continue
        for elem in value:
            if not isinstance(elem, str):
                value.remove(elem)
                value.add(str(elem))
        try:
            column = DiscreteVariable(key, values=value)
        except Exception as ex:
            traceback.print_exc()
            print(value)
        orange_columns.append(column)
    # if use_resources:
    #     resource_encoder = OrangeTableResourceColumnGenerator(mongo_query)
    #     resource_columns = resource_encoder.get_table_columns()
    #     orange_columns.extend(resource_columns)
    # else:
    resource_encoder = None
    domain = Domain(orange_columns)

    records = 0
    table = Table(domain)
    paginator = helpers.scan(es, query={"query": {"match_all": {}}}, index=all_logs_index, doc_type='doc')
    for hit in paginator:
        instance = createInstance(domain, hit['_source'], resource_encoder, prune_null_resources)
        table.append(instance)
        records += 1
        # if records % 1000 == 0:
        #     print('Records : ' + str(records))
    # print('Built Table: %d recrods' % len(table))
    return table, single_value_columns

# def build_orange_tablefrom_normalized_events(query):
#     field_values = {}
#     records = 0
#     for event in events_collection.find(query):
#         records += 1
#         # if records % 1000 == 0:
#         #     print('Records : ' + str(records))
#         for key, value in event.items():
#             if key == '_id' or key.startswith('meta'):
#                 continue
#             RuleUtils.addMulti(field_values, key, value)
#
#     for k in sorted(field_values, key=lambda k: len(field_values[k]), reverse=True):
#             print(k, len(field_values[k]))
#
#     orange_columns = []
#     for key, value in field_values.items():
#         # if (len(value) < 2 or len(value) >= 200) and key != 'eventName':
#         #     continue
#         # if key.startswith('response'): # or key.startswith('request'):
#         #     continue
#         # if key.endswith('Modified') or key.endswith('created') or key.endswith('Time') or key.endswith("Date"):
#         #     continue
#         # if 'Byte' in key or 'policy' in key or 'Policy' in key or 'imageManifest' in key:
#         #     continue
#         # if key == 'requestID' or key == 'eventID' or key == 'userIdentity_arn' or key == 'userIdentity_principalId' or key == 'userIdentity_invokedBy':
#         #     continue
#         try:
#             column = DiscreteVariable(key, values=value)
#         except:
#             print(value)
#         orange_columns.append(column)
#     domain = Domain(orange_columns)
#
#     records = 0
#     table = Table(domain)
#     for event in events_collection.find(query):
#         event.pop('_id')
#         event.pop('meta_event_hash')
#         instance = createInstance(field_values, domain, event)
#         table.append(instance)
#         records += 1
#         # if records % 1000 == 0:
#         #     print('Records : ' + str(records))
#     return table


if __name__ == "__main__":
    date_start = dateutil.parser.parse('2017-10-01T00:00:00Z')
    date_end = dateutil.parser.parse('2017-10-31T23:59:59Z')
    query = config.mongo_source_query
    # table = build_orange_table(query)
    table = build_orange_table_from_es_logs(query)
    Table.save(table, 'table.tab')
