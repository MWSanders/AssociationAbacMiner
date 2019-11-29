import itertools
import math
from datetime import datetime
import traceback


class ParallelEventEnumerator(object):
    def __init__(self, param_info, partition_field, workers=8):
        self.param_info = param_info
        self.workers = workers
        largest_field_count = 0
        self.partition_field = partition_field
        # for k, v in param_info['possible_params'].items():
        #     if len(v) > largest_field_count:
        #         largest_field_count = len(v)
        #         self.partition_field = k

    # def __next__(self):
    #     return self.generate_events()


    def process(self, param_info, override_params, partition_field):
        event_enumerator = EventEnumerator(param_info)
        events = []
        for event in event_enumerator.generate_events(override_params, partition_field):
            events.append(event)
        return events

    def generate_events(self, pool):
        inputs = []
        chunk_size = math.ceil(len(self.param_info['possible_params'][self.partition_field]) / self.workers)
        idx = 0
        while idx < len(self.param_info['possible_params'][self.partition_field]):
            override_params = itertools.islice(self.param_info['possible_params'][self.partition_field], idx, idx+chunk_size)
            inputs.append((self.param_info, set(override_params), self.partition_field))
            idx += chunk_size
        results = pool.starmap(self.process, inputs)
        return itertools.chain(*results)


class EventEnumerator(object):
    def __init__(self, param_info):
        self.param_info = param_info
        self.binned_fields = param_info['event_normalizer_params']['fields_to_bin']

    def __next__(self):
        return self.generate_events()

    def generate_events(self, override_params=None, partition_field=None):
        start = datetime.now()
        new_event = {}
        total_events_generated = [1]
        total_events_generated[0] = 0
        for event in self.dfs_event(new_event, self.param_info['rtopo_sorted_keys'], self.param_info['possible_params'], self.param_info['inv_param_dependency_mmap'], 0, total_events_generated, override_params, partition_field):
            yield event
        time_elapsed = datetime.now() - start
        # print('Total events generated: %d in %ds' % (total_events_generated[0], time_elapsed.total_seconds()))


    def dfs_event(self, event, keys, possible_params, param_dependency_mmap, depth, total_events_generated, override_params, partition_field):
        current_key = keys[depth]
        # print('%d:%s' % (depth, current_key))
        if override_params and current_key == partition_field:
            possible_values = set(override_params)
        else:
            possible_values = set(possible_params[current_key])
        if current_key in param_dependency_mmap.keys():
            chosen_keys = set(event.keys())
            current_key_dependencies = set(param_dependency_mmap[current_key].keys())
            dependency_intersection_keys = chosen_keys.intersection(current_key_dependencies)
            if dependency_intersection_keys:
                for dependent_key in dependency_intersection_keys:
                    if any([dependent_key.startswith(l) for l in self.binned_fields]) and (dependent_key not in event or event[dependent_key] == 'NONE'):
                        continue
                    # try:
                    #     if event[dependent_key] in param_dependency_mmap[current_key][dependent_key]:
                    dependent_values = param_dependency_mmap[current_key][dependent_key][event[dependent_key]]
                    possible_values.intersection_update(dependent_values)
                    # except:
                    #     print(traceback.format_exc())
                    if not possible_values:
                        break
        for current_value in possible_values:
            event[current_key] = current_value
            if depth == len(keys) - 1:
                total_events_generated[0] += 1
                if total_events_generated[0] % 100000 == 0:
                    print('%s Total events enumerated: %d' % (datetime.utcnow(), total_events_generated[0]))
                yield event.copy()
            else:
                yield from self.dfs_event(event, keys, possible_params, param_dependency_mmap, depth+1, total_events_generated, override_params, partition_field)
            event.pop(current_key)