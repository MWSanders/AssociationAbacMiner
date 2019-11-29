import geoip2.database
import pymongo
from src.config import config
from collections import Counter
import ipaddress

client = config.client
events_collection = config.events_collection
jobs_collection = config.jobs_collection
service_op_resource_collection = config.service_op_resource_collection


reader = geoip2.database.Reader('resources/GeoLite2-City.mmdb')
location_counter = Counter()
event_count = 0
for event in events_collection.find({}, {'sourceIPAddress'}):
    event_count += 1
    if event_count % 50000 == 0:
        print(event_count)
        print(location_counter)
    try:
        is_valid = ipaddress.IPv4Address(event['sourceIPAddress'])
        response = reader.city(event['sourceIPAddress'])
        if not response.country.iso_code == 'US':
            location_counter.update([response.country.iso_code])
        else:
            if not response.subdivisions.most_specific.iso_code:
                location_counter.update(['US:UNKNOWN'])
            else:
                location_counter.update(['US:' + response.subdivisions.most_specific.iso_code])
    except:
        location_counter.update([event['sourceIPAddress']])

print(location_counter)
print()