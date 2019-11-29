import dateutil
import re
from src.model import event_flattner

class ConfigurableEventNormalizerNg(object):
    def __init__(self, use_resources=None, bin_method='simple-6', fields_to_bin=set(), valid_keys=None, add_missing_fields=False, pop_binned_fields=False):
        self.valid_keys = valid_keys
        self.add_missing_fields = add_missing_fields
        self.fields_to_bin = fields_to_bin
        self.use_resources = use_resources
        self.bin_method = bin_method
        self.pop_binned_fields = pop_binned_fields
        if self.pop_binned_fields and fields_to_bin:
            for key in fields_to_bin:
                if key == 'eventName':
                    continue
                self.valid_keys.discard(key)
        # if 'eventTime' in self.fields_to_bin:
        #     self.valid_keys.add('eventTime_weekday')
        #     self.valid_keys.add('eventTime_weekend')
        #     self.valid_keys.add('eventTime_bin')
        # if 'sourceIPAddress' in self.fields_to_bin:
        #     self.valid_keys.add('sourceIPAddress_bin')
        #     self.valid_keys.add('sourceIPAddress_trunc')
        #     if pop_binned_fields:
        #         self.valid_keys.discard('sourceIPAddress')
        # if 'eventName' in self.fields_to_bin:
        #     self.valid_keys.add('eventName_bin')
        #     self.valid_keys.add('eventName_crud_bin')
            # if pop_binned_fields:
            #     self.valid_keys.discard('eventName')
        # if 'userAgent' in self.fields_to_bin:
        #     self.valid_keys.add('userAgent_bin')
        #     self.valid_keys.add('userAgent_general_bin')
        #     if pop_binned_fields:
        #         self.valid_keys.discard('userAgent')


        self.hour_bins = {}
        self.hour_bins['eqf-8'] = {0: [17], 1: [18, 19], 2: [20, 21], 3: [22, 23, 0], 4: [1, 2, 3, 4, 5, 6], 5: [7, 8, 9, 10, 11, 12, 13], 6:[14, 15], 7:[16]}
        self.hour_bins['eqf-6'] = {0: [16, 17], 1: [18, 19], 2:[20, 21, 22], 3: [23, 0, 1, 2, 3], 4: [4, 5, 6, 7, 8, 9, 10, 11, 12], 5:[13, 14, 15]}
        self.hour_bins['eqf-5'] = {0: [16, 17], 1: [18, 19, 20], 2: [21, 22, 23, 0], 3: [1, 2, 3, 4, 5, 6, 7, 8, 9], 4: [10, 11, 12, 13, 14, 15]}
        self.hour_bins['eqf-4'] = {0: [16, 17, 18], 1: [19, 20, 21], 2: [22, 23, 0, 1, 2, 3, 4, 5, 6, 7], 3: [8, 9, 10, 11, 12, 13, 14, 15]}
        self.hour_bins['eqf-3'] = {0: [15, 16, 17, 18], 1: [19, 20, 21, 22, 23, 0], 2: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]}
        self.hour_bins['eqf-2'] = {0: [14, 15, 16, 17, 18, 19, 20, 21], 1: [22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]}
        self.hour_bins['eqw-8'] = {0: [10, 11, 12], 1: [13, 14, 15], 2: [16, 17, 18], 3:[19, 20, 21], 4:[22, 23, 0], 5:[1, 2, 3], 6:[4, 5, 6], 7:[7, 8, 9]}
        self.hour_bins['eqw-6'] = {0: [10, 11, 12, 13], 1: [14, 15, 16, 17], 2: [18, 19, 20, 21], 3: [22, 23, 0, 1], 4: [2, 3, 4, 5], 5:[6, 7, 8, 9]}
        self.hour_bins['eqw-4'] = {0: [10, 11, 12, 13, 14, 15], 1: [16, 17, 18, 19, 20, 21], 2: [22, 23, 0, 1, 2, 3], 3: [4, 5, 6, 7, 8, 9]}
        self.hour_bins['eqw-3'] = {0: [10, 11, 12, 13, 14, 15, 16, 17], 1: [18, 19, 20, 21, 22, 23, 0, 1], 2: [2, 3, 4, 5, 6, 7, 8, 9]}
        self.hour_bins['eqw-2'] = {0: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21], 1: [22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
        self.inv_hour_bins = {}
        for bin_method_key, bin_method_value in self.hour_bins.items():
            if bin_method_key not in self.inv_hour_bins: self.inv_hour_bins[bin_method_key] = {}
            for bucket_key, hour_list in bin_method_value.items():
                for hour in hour_list:
                    self.inv_hour_bins[bin_method_key][hour] = bucket_key
        self.create_fields = ['create']
        self.read_fields = ['list', 'get', 'subscribe', 'search', 'scan', 'estimate', 'describe', 'check']
        self.delete_fields = ['delete', 'remove']
        self.execute_fields = ['run', 'start', 'retry', 'batch', 'stop', 'reboot', 'terminate', 'deactivate']
        self.bad_keys = set()

    @classmethod
    def from_param_info(cls, param_info):
        return cls(use_resources=param_info['event_normalizer_params']['use_resources'], bin_method=param_info['event_normalizer_params']['bin_method'], fields_to_bin=param_info['event_normalizer_params']['fields_to_bin'],
                   valid_keys=param_info['event_normalizer_params']['valid_keys'], add_missing_fields=param_info['event_normalizer_params']['add_missing_fields'], pop_binned_fields=param_info['event_normalizer_params']['pop_binned_fields'])

    def bin_hour(self, hour):
        if self.bin_method == 'simple-8':
            return (int(hour / 3)) # 3 hour buckets = 8 buckets total
        elif self.bin_method == 'simple-6':
            return (int(hour / 4)) # 4 hour buckets = 6 buckets total
        elif self.bin_method == 'simple-4':
            return (int(hour / 6)) # 6 hour buckets = 4 buckets total
        elif self.bin_method == 'simple-3':
            return (int(hour / 8)) # 8 hour buckets = 3 buckets total
        elif self.bin_method == 'simple-2':
            return (int(hour / 12)) # 12 hour buckets = 2 buckets total
        return self.inv_hour_bins[self.bin_method][hour]

    def normalized_user_op_resource_from_event(self, event):
        event.pop('_id', None)
        event = event_flattner.flatten(event, "_")

        if 'userIdentity_invokedBy' in event:
            raw_invokedBy = event['userIdentity_invokedBy']
            if 'signin__amazonaws__com' == raw_invokedBy:
                pass
            elif 'amazonaws' in raw_invokedBy or 'internal' in raw_invokedBy.lower():
                event['userIdentity_invokedBy'] = 'internal'
        if 'eventTime' in self.fields_to_bin and 'eventTime' in event:
            eventTime = dateutil.parser.parse(event['eventTime'])
            weekday = str(eventTime.weekday())
            if weekday == '5' or weekday == '6':
                weekend = 'true'
            else:
                weekend = 'false'
            event['eventTime_weekday'] = weekday
            event['eventTime_weekend'] = weekend
            event['eventTime_bin'] = str(self.bin_hour(eventTime.hour))
        event.pop('eventTime', None)

        if 'sourceIPAddressBin' in event:
            if event['sourceIPAddressBin'].startswith('10.'):
                event['sourceIPAddressBin'] = 'vpc.internal'
        if 'sourceIPAddressInternal' in event:
            if event['sourceIPAddressInternal'].startswith('10.'):
                event['sourceIPAddressInternal'] = 'vpc.internal'
        if 'sourceIPAddress' in self.fields_to_bin and 'sourceIPAddress' in event:
            if event['sourceIPAddress'].endswith('amazonaws__com'):
                event['sourceIPAddress_bin'] = 'amazonaws__com'
            elif event['sourceIPAddress'].endswith('internal'):
                event['sourceIPAddress_bin'] = 'internal'
            else:
                event['sourceIPAddress_bin'] = 'external'
            event['sourceIPAddress_trunc'] = event['sourceIPAddress'].split('__')[0]
            if 'amazonaws' in event['sourceIPAddress'] or 'internal' in event['sourceIPAddress'].lower():
                event['sourceIPAddress'] = 'internal'
            if self.pop_binned_fields:
                event.pop('sourceIPAddress')
        if 'eventName' in self.fields_to_bin and 'eventName' in event:
            match = re.search(r'^([^A-Z]*[A-Z]){2}', event['eventName'])
            try:
                idx = match.span()[1]
                event['eventName_bin'] = event['eventName'][0:idx-1]
            except:
                event['eventName_bin'] = event['eventName']

            if any(map(event['eventName_bin'].lower().startswith, self.read_fields)):
                event['eventName_crud_bin'] = 'Read'
            elif any(map(event['eventName_bin'].lower().startswith, self.execute_fields)):
                event['eventName_crud_bin'] = 'Execute'
            elif any(map(event['eventName_bin'].lower().startswith, self.create_fields)):
                event['eventName_crud_bin'] = 'Create'
            elif any(map(event['eventName_bin'].lower().startswith, self.delete_fields)):
                event['eventName_crud_bin'] = 'Delete'
            else:
                event['eventName_crud_bin'] = 'Update'
        # if self.pop_binned_fields and 'eventName' in self.fields_to_bin :
        #     event.pop('eventName')
        # else:
        event['eventName'] = '%s:%s' % (event['eventSource'], event['eventName'])

        agent_general_bin = self.bin_bin_userAgent(event['userAgent'])
        if 'userAgent' in self.fields_to_bin and 'userAgent' in event:
            event['userAgent_bin'] = self.bin_userAgent(event['userAgent'])
            event['userAgent_general_bin'] = self.bin_bin_userAgent(event['userAgent'])
            if 'signin__amazonaws__com' == event['userAgent']:
                pass
            elif 'amazonaws' in event['userAgent'] or 'internal' in event['userAgent'].lower():
                event['userAgent'] = 'internal'
            if self.pop_binned_fields:
                event.pop('userAgent')
        # Remove ephemeral access keys not associated with CLI or SDK calls
        if agent_general_bin not in ['aws-sdk', 'aws-cli']:
            event.pop('userIdentity_accessKeyId', None)
        if self.add_missing_fields and self.valid_keys:
            for k in self.valid_keys:
                if k not in event and k in self.valid_keys:
                    event[k] = 'NONE'
        keys_to_remove = set()
        for k in event.keys():
            if k.startswith('responseElements'):
                keys_to_remove.add(k)
            if self.valid_keys and k not in self.valid_keys:
                keys_to_remove.add(k)
        for k in keys_to_remove:
            event.pop(k)
        return event

    def bin_bin_userAgent(self, rawUserAgent):
        if 'amazonaws__com' in rawUserAgent:
            userAgent_general = 'amazonaws__com'
        elif 'aws-sdk' in rawUserAgent or 'Boto' in rawUserAgent:
            userAgent_general = 'aws-sdk'
        elif 'aws-cli' in rawUserAgent:
            userAgent_general = 'aws-cli'
        elif 'internal' in rawUserAgent.lower():
            userAgent_general = 'internal'
        elif 'Web' in rawUserAgent or 'Mobile' in rawUserAgent or 'Mozilla' in rawUserAgent:
            userAgent_general = 'console'
        else:
            userAgent_general = 'other'

        return userAgent_general

    def bin_userAgent(self, rawUserAgent):
        if 'Mobile' in rawUserAgent:
            userAgent = 'Mobile'
        elif 'Boto' in rawUserAgent:
            userAgent = 'Boto'
        elif 'aws-sdk-java' in rawUserAgent:
            userAgent = 'aws-sdk-java'
        elif 'Mozilla' in rawUserAgent or 'Web' in rawUserAgent:
            userAgent = 'Web'
        elif 'aws-cli' in rawUserAgent:
            userAgent = 'aws-cli'
        elif 'aws-sdk-go' in rawUserAgent:
            userAgent = 'aws-sdk-go'
        elif rawUserAgent.startswith('aws-sdk-nodejs'):
            userAgent = 'aws-sdk-nodejs'
        elif 'Transmit' in rawUserAgent:
            userAgent = 'Transmit'
        elif 'internal' in rawUserAgent or 'Internal' in rawUserAgent:
            userAgent = 'AWS Internal'
        elif 'amazonaws__com' in rawUserAgent:
            userAgent = rawUserAgent
        else:
            userAgent = 'other'
        return userAgent

if __name__ == "__main__":
    pass