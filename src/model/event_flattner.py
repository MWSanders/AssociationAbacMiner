
import six

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
            bad_keys.add(key)
            # flattened_dict.pop(key)
            # flattened_dict[key] = object_
        # These object types support iteration
        elif isinstance(object_, dict):
            for object_key in object_:
                if not (not key and object_key in root_keys_to_ignore):
                    _flatten(object_[object_key], _construct_key(key,
                                                                 separator,
                                                                 object_key))
        elif isinstance(object_, list) or isinstance(object_, set):
            return # TODO explode lists
            # for index, item in enumerate(object_):
            #     _flatten(item, _construct_key(key, separator, index))
        # Anything left take as is
        else:
            flattened_dict[key] = str(object_).replace('\t', ' ').replace('.', '__')
            if len(str(object_).replace('\t', ' ')) > 512:
                bad_keys.add(key)

    _flatten(nested_dict, None)
    # if 'requestParameters_iam_roleName' in flattened_dict:
    #     flattened_dict['requestParameters_iam_roleName'] = '-'.join(flattened_dict['requestParameters_iam_roleName'].split('-')[:-1])
    # if 'requestParameters_cloudformation_stackName' in flattened_dict:
    #     if '/' in flattened_dict['requestParameters_cloudformation_stackName']:
    #         flattened_dict['requestParameters_cloudformation_stackName'] = '/'.join(flattened_dict['requestParameters_cloudformation_stackName'].split('/')[1:-1])
    # if 'requestParameters_ecs_taskDefinition' in flattened_dict:
    #     flattened_dict['requestParameters_ecs_taskDefinition'] = '-'.join(flattened_dict['requestParameters_ecs_taskDefinition'].split('-')[:-1])
    # # if 'requestParameters_ecs_family' in flattened_dict:
    #     # flattened_dict['requestParameters_ecs_family'] = '-'.join(flattened_dict['requestParameters_ecs_family'].split('-')[:-1])
    # # if 'requestParameters_ecs_taskRoleArn' in flattened_dict:
    # #     flattened_dict['requestParameters_ecs_taskRoleArn'] = '-'.join(flattened_dict['requestParameters_ecs_taskRoleArn'].split('-')[:-1])
    return flattened_dict