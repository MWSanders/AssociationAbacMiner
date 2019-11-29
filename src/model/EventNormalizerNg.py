from src.experiment.OrangeTableResourceColumnGenerator import OrangeTableResourceColumnGenerator
from src.config import config


class EventNormalizerNg(object):
    def __init__(self, use_resources=None, bin_method='simple-6'):
        self.use_resources = use_resources
        self.bin_method = bin_method
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
        #user
        userIdentity = event['userIdentity']
        userName = userIdentity['userName'] if 'userName' in userIdentity else userIdentity['sessionContext']['sessionIssuer']['userName']
        eventType = userIdentity['type']
        accessKey = userIdentity['accessKeyId'] if userIdentity['type'] == 'IAMUser' and 'accessKeyId' in userIdentity and 'invokedBy' not in userIdentity  else 'None'
        accountId = userIdentity['accountId']
        rawUserAgent = event['userAgent']

        userAgent = self.bin_userAgent(rawUserAgent)
        userAgent_general = self.bin_bin_userAgent(rawUserAgent)
        #op
        eventTime = event['eventTime']
        weekday = str(eventTime.weekday())
        if weekday == '5' or weekday == '6':
            weekend = 'true'
        else:
            weekend = 'false'
        hour_bucket = str(self.bin_hour(eventTime.hour))

        #resource
        resources = []
        if self.use_resources:
            if 'meta_normalizedResourcesPrimary' not in event:
                current_resource = {
                    'resource_id': 'None',
                    'resource_type': 'None'
                }
                resources.append(current_resource)
            else:
                for resource in event['meta_normalizedResourcesPrimary']:
                    resource_parts = resource.split('-')
                    if len(resource_parts) > 2:
                        resource = '%s-%s' % (resource_parts[0], resource_parts[1])
                    current_resource = {
                        'resource_id': resource,
                        'resource_type': resource.split(':')[5].split('/')[0]
                    }
                    resources.append(current_resource)

        mfa_auth = 'false'
        if 'sessionContext' in event['userIdentity']: mfa_auth = event['userIdentity']['sessionContext']['attributes']['mfaAuthenticated']
        if self.use_resources:
            result = {
                'user_name': userName,
                'user_type': eventType,
                'user_accessKey': accessKey,
                'user_account': accountId,
                'resource_op_eventSource': event['eventSource'],
                'resource_op_eventName': event['eventName'],
                'user_op_sourceIPAddress': event['sourceIPAddress'].split('.')[0],
                'user_op_eventType': event['eventType'],
                'user_op_userAgent': userAgent,
                'user_op_userAgent_general': userAgent_general,
                'user_op_mfaAuthenticated': mfa_auth,
                'op_weekday': weekday,
                'op_weekend': weekend,
                'op_hour_bucket': hour_bucket,
                'resources': resources
            }
        else:
            result = {
                'user_name': userName,
                'user_type': eventType,
                'user_accessKey': accessKey,
                'user_account': accountId,
                'op_eventSource': event['eventSource'],
                'op_eventName': event['eventName'],
                'user_op_sourceIPAddress': event['sourceIPAddress'].split('.')[0],
                'user_op_eventType': event['eventType'],
                'user_op_userAgent': userAgent,
                'user_op_userAgent_general': userAgent_general,
                'user_op_mfaAuthenticated': mfa_auth,
                'op_weekday': weekday,
                'op_weekend': weekend,
                'op_hour_bucket': hour_bucket,
                'resources': resources
            }
        return result

    def bin_bin_userAgent(self, rawUserAgent):
        if 'amazonaws.com' in rawUserAgent:
            userAgent_general = 'amazonaws.com'
        elif 'aws-sdk' in rawUserAgent or 'Boto' in rawUserAgent:
            userAgent_general = 'aws-sdk'
        elif 'aws-cli' == rawUserAgent:
            userAgent_general = 'aws-cli'
        elif 'internal' in rawUserAgent or 'Internal' in rawUserAgent:
            userAgent_general = 'internal'
        elif 'Web' in rawUserAgent or 'Mobile' in rawUserAgent or 'Mozilla' in rawUserAgent:
            userAgent_general = 'console'
        else:
            userAgent_general = 'other'

        return userAgent_general

    def bin_userAgent(self, rawUserAgent):
        if 'Mobile' in rawUserAgent:
            userAgent = 'Mobile'
        elif 'Boto3' in rawUserAgent:
            userAgent = 'Boto3'
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
        else:
            userAgent = rawUserAgent
        return userAgent


if __name__ == "__main__":
    gen = EventNormalizerNg(False)
    print(gen.bin_hour(5))
    print(gen.bin_hour(2))
    print(gen.bin_hour(12))