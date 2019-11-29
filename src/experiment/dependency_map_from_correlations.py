from src.model import RuleUtils

correlations = [
    'eventName -> eventName_bin',
    'eventName -> eventSource',
    'eventName -> eventType',
    'eventName -> eventVersion',
    'eventSource -> eventType',
    'eventTime_weekday -> eventTime_weekend',
    'sourceIPAddress -> sourceIPAddress_bin',
    'userAgent_bin -> eventType',
    'userAgent_bin -> sourceIPAddress_bin',
    'userAgent_bin -> userAgent_general_bin',
    'userAgent_bin -> userIdentity_invokedBy',
    'userAgent_general_bin -> eventType',
    'userIdentity_invokedBy -> sourceIPAddress_bin',
    'eventName -> requestParameters_encryptionContext_PARAMETER_ARN',
    'eventName -> requestParameters_path',
    'eventName -> requestParameters_pipelineName',
    'eventName -> apiVersion',
    'eventSource -> eventVersion',
    'eventName -> requestParameters_name',
    'eventName -> requestParameters_maxResults',
    'userIdentity_accessKeyId -> userIdentity_userName',
    'apiVersion -> eventType',
    'apiVersion -> eventVersion',
    'userIdentity_invokedBy -> eventType',
    'apiVersion -> eventSource',
    'userIdentity_invokedBy -> sourceIPAddress',
    'userAgent_bin -> userIdentity_sessionContext_attributes_mfaAuthenticated',
    'sourceIPAddress_bin -> eventType',
    'eventType -> eventVersion',
    'eventType -> userIdentity_accessKeyId',
    'eventType -> userIdentity_sessionContext_attributes_mfaAuthenticated'
]

dependency_mmap = {}
for corr in correlations:
    k1 = corr.split(' -> ')[0]
    k2 = corr.split(' -> ')[1]
    RuleUtils.addMulti(dependency_mmap, k1, k2)

print(dependency_mmap)