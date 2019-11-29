from src.config import config
from src.model import param_universe_dao
from src.model.EventEnumerator import EventEnumerator
from src.model.LogUniverseGenerator import *

class PartitionedUniverseIndexer(object):
    def __init__(self, scoring_param_info_id):
        self.scoring_param_info_id = scoring_param_info_id
        self.es = config.es
        self.scoring_param_info = param_universe_dao.load_universe_info(self.scoring_param_info_id)
        self.op_possible_index = 'flat-op-' + self.scoring_param_info_id.lower()
        self.user_possible_index = 'flat-user-' + self.scoring_param_info_id.lower()
        self.resource_possible_index = 'flat-resource-' + self.scoring_param_info_id.lower()
        self.use_resources = False

    def index(self):
        # Index user, op, resource combinations
        if self.use_resources:
            if not self.es.indices.exists(index=self.resource_possible_index):
                resource_param_info = self.scoring_param_info.copy()
                resource_keys = [s for s in self.scoring_param_info['valid_keys_sets']['valid_keys'] if s.startswith('requestParameters')]
                resource_keys.append('eventName')
                resource_keys.append('eventSource')
                param_universe_dao.prune_param_info(resource_param_info, resource_keys)
                resource_event_enumerator = EventEnumerator(resource_param_info)
                resource_es_writer = FlatEsUniverseWriter(resource_event_enumerator)
                resource_es_writer.index_separated_universes(self.resource_possible_index)

        if not self.es.indices.exists(index=self.user_possible_index):
            user_param_info = self.scoring_param_info.copy()
            param_universe_dao.prune_param_info(user_param_info, self.scoring_param_info['valid_keys_sets']['valid_keys_user'])
            if user_param_info['rtopo_sorted_keys']:
                user_event_enumerator = EventEnumerator(user_param_info)
                user_es_writer = FlatEsUniverseWriter(user_event_enumerator)
                user_es_writer.index_separated_universes(self.user_possible_index)
            else:
                user_es_writer = FlatEsUniverseWriter(None)
                user_es_writer.create_index_only(self.user_possible_index)

        if not self.es.indices.exists(index=self.op_possible_index):
            resource_param_info = self.scoring_param_info.copy()
            op_keys = self.scoring_param_info['valid_keys_sets']['valid_keys_op']
            if self.use_resources:
                keys_to_remove = set()
                for key in op_keys:
                    if key.startswith('requestParameters'):
                        keys_to_remove.add(key)
                for k in keys_to_remove:
                    op_keys.discard(k)
            param_universe_dao.prune_param_info(resource_param_info, op_keys)
            resource_event_enumerator = EventEnumerator(resource_param_info)
            resource_es_writer = FlatEsUniverseWriter(resource_event_enumerator)
            resource_es_writer.index_separated_universes(self.op_possible_index)

    def count_possible_privs(self):
        #store to total_possible_priv_states
        total_priv_size = 0
        event_names = self.scoring_param_info['possible_params']['eventName']
        user_count = es.count(index=self.user_possible_index, doc_type='doc', body={ "query": {"match_all" : { }}})['count']
        if not self.use_resources:
            op_count = es.count(index=self.op_possible_index, doc_type='doc', body={ "query": {"match_all" : { }}})['count']
            return user_count * op_count
        else:
            for event_name in event_names:
                op_count = es.count(index=self.op_possible_index, doc_type='doc', body={ "query": {"match":{"eventName":event_name }}})['count']
                resource_count = es.count(index=self.resource_possible_index, doc_type='doc', body={ "query": {"match":{"eventName":event_name }}})['count']
                total_priv_size += user_count * op_count * resource_count

            # queries = []
            # req_head = {'index': self.resource_possible_index, 'type': 'doc'}
            # if user_constraints:
            #     user_query = RuleUtils.create_terms_filter_from_constraints(user_constraints)
            # else:
            #     user_query = {"query": {"match_all": {}}, "size": 0}
            # queries.extend([req_head, user_query])

            return  total_priv_size



if __name__ == "__main__":
    # indexer = PartitionedUniverseIndexer('fields_large_ps_amftrue_pbftrue')
    # indexer = PartitionedUniverseIndexer('fields_small_ps_amftrue_pbftrue')
    indexer = PartitionedUniverseIndexer('fields_small_ps_amftrue_pbffalse')
    indexer.index()
    print(indexer.count_possible_privs())