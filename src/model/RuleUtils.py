
def addMulti(nested_dict, key, value):
    if isinstance(value, list):
        for item in value:
            addMulti(nested_dict, key, item)
    elif isinstance(value, dict):
        for sub_key, sub_value in value.items():
            new_key = key + "." + sub_key
            addMulti(nested_dict, new_key, sub_value)
    elif isinstance(value, bool):
        if key not in nested_dict:
            nested_dict[key] = set()
        current_set = nested_dict[key]
        current_set.add(value)
    else:
        if key not in nested_dict:
            nested_dict[key] = set()
        current_set = nested_dict[key]
        try:
            current_set.add(str(value))
        except:
            pass

def has_same_attributes(rule1, rule2):
    if rule1.constraints_map.keys() == rule2.constraints_map.keys():
        return True
    return False

def item_to_constraint_set(item, mapping, names):
    indexes = set([index for index in item])
    column_value_map = {}
    for index in indexes:
        column, value = mapping[index]
        column_value_map[column] = value
    rule_contrains_names = set()
    for index in indexes:
        rule_contrains_names.add(names[index])
    return rule_contrains_names

# def merge_rules(rule1, rule2):
#     merged_rule = RuleEval()
#     # merged_rule_mmap = {}
#     # for key, value in rule1.constraints_map.items():
#     #     LogUniverseBuilder.addMulti(merged_rule_mmap, key, value)
#     # for key, value in rule2.constraints_map.items():
#     #     LogUniverseBuilder.addMulti(merged_rule_mmap, key, value)
#     # merged_rule.constraints_map = merged_rule_mmap
#
#     merged_rule_constraints = set()
#     merged_rule_constraints.update(rule1.constraints)
#     merged_rule_constraints.update(rule2.constraints)
#     merged_rule.constraints = merged_rule_constraints
#
#     merged_rule.constraints_map = create_terms_mmap_from_constraints(merged_rule_constraints)
#
#     #TODO calculate metrics from applying the rules to logs, not just averages
#     merged_rule.all_log_entries = rule1.count + rule2.count
#     merged_rule.coverate_rate = (rule1.coverage_rate + rule2.coverage_rate)/2
#     merged_rule.overassignment_rate = (rule1.overassignment_rate + rule2.overassignment_rate)/2
#     merged_rule.sort_metric_value = (rule1.sort_metric_value + rule2.sort_metric_value)/2
#     return merged_rule


def constraint_set_to_multimap(constraints):
    constraint_mmap = {}
    for constraint in constraints:
        key = constraint.split('=')[0]
        value = constraint.split('=')[1]
        addMulti(constraint_mmap, key, value)
    return constraint_mmap

def map_to_constraint_set(constraints_map):
    constraint_set = set()
    for key, value in constraints_map.items():
        constraint_set.add('%s=%s' % (key, value))
    return constraint_set

def multimap_to_query(constraints_map):
    pass

def create_terms_filter_from_constraints(rule_constraint_names):
    query = {"query": {"bool": {"filter": []}}}
    filter_list = query['query']['bool']['filter']
    terms_mmap = create_terms_mmap_from_constraints(rule_constraint_names)
    for key, values in terms_mmap.items():
        terms_filter = {}
        terms_filter['terms'] = {}
        for value in values:
            addMulti(terms_filter['terms'], key, value)
        terms_filter['terms'][key] = list(terms_filter['terms'][key])
        filter_list.append(terms_filter)
    query['size'] = 0
    return query

def create_query_convert_resource_to_type(rule_constraint_names):
    query = {"query": {"bool": {"filter": [], "must_not": []}}}
    filter_list = query['query']['bool']['filter']
    resource_type_must_not_list = query['query']['bool']['must_not']
    resource_not_filter = {}
    resource_not_filter['terms'] = {}
    resource_not_filter['terms']['resources.resource_type'] = set()

    terms_mmap = create_terms_mmap_from_constraints(rule_constraint_names)

    for key, values in terms_mmap.items():
        terms_filter = {}
        terms_filter['terms'] = {}
        for value in values:
            if key.startswith('resource_') and not key.startswith('resource_op'):
                key_parts = key.split('_')
                key = 'resources.resource_type'
                if value.startswith('arn'):
                    addMulti(terms_filter['terms'], 'resources.resource_id', value)
                elif value == 'None':
                    addMulti(resource_not_filter['terms'], 'resources.resource_type', key_parts[1])
            else:
                addMulti(terms_filter['terms'], key, value)
        if key in terms_filter['terms']:
            terms_filter['terms'][key] = list(terms_filter['terms'][key])
            filter_list.append(terms_filter)
        if 'resources.resource_id' in terms_filter['terms']:
            terms_filter['terms']['resources.resource_id'] = list(terms_filter['terms']['resources.resource_id'])
            filter_list.append(terms_filter)

    if resource_not_filter['terms']['resources.resource_type']:
        resource_not_filter['terms']['resources.resource_type'] = list(resource_not_filter['terms']['resources.resource_type'])
        resource_type_must_not_list.append(resource_not_filter)
    query['size'] = 0
    return query

def op_create_terms_filter_from_constraints(rule_constraint_names, invalid_events):
    query = {"query": {"bool": {"filter": [], "must_not": []}}}
    filter_list = query['query']['bool']['filter']
    must_not_list = query['query']['bool']['must_not']
    terms_mmap = create_terms_mmap_from_constraints(rule_constraint_names)
    for key, values in terms_mmap.items():
        terms_filter = {}
        terms_filter['terms'] = {}
        terms_filter['terms'][key] = list(terms_mmap[key])
        filter_list.append(terms_filter)
    query['size'] = 0
    return query

def resource_create_terms_filter_from_constraints(rule_constraint_names, invalid_events, invalid_types):
    query = {"query": {"bool": {"filter": [], "must_not": []}}}
    filter_list = query['query']['bool']['filter']
    must_not_list = query['query']['bool']['must_not']
    terms_mmap = create_terms_mmap_from_constraints(rule_constraint_names)
    for key, values in terms_mmap.items():
        if 'None' in values:
            pass
        else:
            terms_filter = {}
            resource_filter = {}
            terms_filter['terms'] = {}
            resource_filter['terms'] = {}
            for value in values:
                if value.startswith('arn:'):
                    addMulti(resource_filter['terms'], 'resources.resource_id', value)
                else:
                    addMulti(terms_filter['terms'], key, value)
            if 'resources.resource_id' in resource_filter['terms']:
                resource_filter['terms']['resources.resource_id'] = list(resource_filter['terms']['resources.resource_id'])
                filter_list.append(resource_filter)
            if key in terms_filter['terms']:
                terms_filter['terms'][key] = list(terms_filter['terms'][key])
                filter_list.append(terms_filter)
    # if invalid_events: #TODO might be able to cut out and just filter on types only?
    #     must_not_filter = {}
    #     must_not_filter['terms'] = {}
    #     must_not_filter['terms']['resource_op_eventName'] = []
    #     for event in invalid_events:
    #         must_not_filter['terms']['resource_op_eventName'].append(event)
    #     must_not_list.append(must_not_filter)
    if invalid_types:
        must_not_filter = {}
        must_not_filter['terms'] = {}
        must_not_filter['terms']['resources.resource_type'] = []
        for resource_type in invalid_types:
            must_not_filter['terms']['resources.resource_type'].append(resource_type)
        must_not_list.append(must_not_filter)
    query['size'] = 0
    return query

def create_terms_mmap_from_constraints(rule_constraint_names):
    terms_mmap = {}
    for constraint in rule_constraint_names:
        if len(constraint.split('=')) < 2:
            print()
        key = constraint.split('=')[0]
        value = constraint.split('=')[1]
        addMulti(terms_mmap, key, value)
    return terms_mmap
