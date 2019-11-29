import json
valid_keys = {'sourceIPAddress', 'sourceIPAddress_bin', 'eventName', 'eventSource', 'eventName_bin', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userAgent_bin', 'userAgent_general_bin',
              'userIdentity_invokedBy', 'eventType', 'sourceIPAddress_bin', 'eventVersion', 'apiVersion', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name',
              'requestParameters_pipelineName', 'requestParameters_maxResults', 'userIdentity_accessKeyId', 'eventTime_weekend', 'eventTime_weekday', 'eventTime_bin', 'userIdentity_userName'
              }

template = {
    "index_patterns": ["flat-user-possible", 'flat-op-possible'],
    "mappings": {
        "doc": {
            "dynamic": "strict",
            "properties": {
                "user_op_mfaAuthenticated": {
                    "type": "keyword"
                },
                "user_op_sourceIPAddress": {
                    "type": "keyword"
                },
                "user_op_userAgent": {
                    "type": "keyword"
                },
                "user_op_userAgent_general": {
                    "type": "keyword"
                },
                "user_accessKey": {
                    "type": "keyword"
                },
                "user_account": {
                    "type": "keyword"
                },
                "user_name": {
                    "type": "keyword"
                },
                "user_type": {
                    "type": "keyword"
                },
                "user_op_eventType": {
                    "type": "keyword"
                }
            }
        }
    }
}

for k in valid_keys:
    template['mappings']['doc']['properties'][k] = {"type": "keyword"}

print(json.dumps(template, indent=4, sort_keys=True))
