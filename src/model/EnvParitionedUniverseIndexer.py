from src.config import config
from src.model import param_universe_dao
from src.model.EventEnumerator import EventEnumerator
from src.model.LogUniverseGenerator import *
import os
from pathlib import Path
import time

class EnvPartitionedUniverseIndexer(object):
    def __init__(self, scoring_param_info_id):
        self.scoring_param_info_id = scoring_param_info_id
        self.es = config.es
        self.scoring_param_info = param_universe_dao.load_universe_info(self.scoring_param_info_id)
        self.op_possible_index = 'flat-op-' + self.scoring_param_info_id.lower()
        self.user_possible_index = 'flat-user-' + self.scoring_param_info_id.lower()
        self.env_possible_index = 'flat-env-' + self.scoring_param_info_id.lower()
        self.use_env = True

    def index(self):
        # Index user, op, resource combinations
        if self.use_env:
            if not self.es.indices.exists(index=self.env_possible_index):
                resource_param_info = self.scoring_param_info.copy()
                resource_keys = self.scoring_param_info['valid_keys_sets']['valid_keys_env']
                param_universe_dao.prune_param_info(resource_param_info, resource_keys)
                resource_event_enumerator = EventEnumerator(resource_param_info)
                resource_es_writer = FlatEsUniverseWriter(resource_event_enumerator)
                resource_es_writer.index_separated_universes(self.env_possible_index)

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
            if self.use_env:
                keys_to_remove = set()
                for s in op_keys:
                    if s.startswith('sourceIPAddress') or s.startswith('userAgent') or s.startswith('userIdentity_invokedBy'):
                        keys_to_remove.add(s)
                for k in keys_to_remove:
                    op_keys.discard(k)
            param_universe_dao.prune_param_info(resource_param_info, op_keys)
            resource_event_enumerator = EventEnumerator(resource_param_info)
            resource_es_writer = FlatEsUniverseWriter(resource_event_enumerator)
            resource_es_writer.index_separated_universes(self.op_possible_index)
        return self.count_indexed_privs()

    def count_correct_privs(self):
        env_count = 0
        user_count = 0
        op_count = 0
        if self.use_env:
            resource_param_info = self.scoring_param_info.copy()
            resource_keys = self.scoring_param_info['valid_keys_sets']['valid_keys_env']
            param_universe_dao.prune_param_info(resource_param_info, resource_keys)
            resource_event_enumerator = EventEnumerator(resource_param_info)
            for env in resource_event_enumerator.generate_events():
                env_count += 1
        else:
            env_count = 1

        user_param_info = self.scoring_param_info.copy()
        param_universe_dao.prune_param_info(user_param_info, self.scoring_param_info['valid_keys_sets']['valid_keys_user'])
        user_event_enumerator = EventEnumerator(user_param_info)
        for user in user_event_enumerator.generate_events():
            user_count += 1

        resource_param_info = self.scoring_param_info.copy()
        op_keys = self.scoring_param_info['valid_keys_sets']['valid_keys_op']
        if self.use_env:
            keys_to_remove = set()
            for s in op_keys:
                if s.startswith('sourceIPAddress') or s.startswith('userAgent') or s.startswith('userIdentity_invokedBy'):
                    keys_to_remove.add(s)
            for k in keys_to_remove:
                op_keys.discard(k)
        param_universe_dao.prune_param_info(resource_param_info, op_keys)
        op_event_enumerator = EventEnumerator(resource_param_info)
        for op in op_event_enumerator.generate_events():
            op_count += 1

        total_priv_size = user_count * op_count * env_count
        return total_priv_size, user_count, op_count, env_count

    def count_indexed_privs(self):
        #store to total_possible_priv_states
        total_priv_size = 0
        # event_names = self.scoring_param_info['possible_params']['eventName']
        user_count = es.count(index=self.user_possible_index, doc_type='doc', body={ "query": {"match_all" : { }}})['count']
        user_count = user_count if user_count > 0 else 1
        op_count = es.count(index=self.op_possible_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
        op_count = op_count if op_count > 0 else 1
        if not self.use_env:
            return user_count * op_count, user_count, op_count, 1
        else:
            env_count = es.count(index=self.env_possible_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
            env_count = env_count if env_count > 0 else 1
            total_priv_size = user_count * op_count * env_count
            return  total_priv_size, user_count, op_count, env_count



if __name__ == "__main__":
    # indexer = PartitionedUniverseIndexer('fields_large_ps_amftrue_pbftrue')
    # indexer = PartitionedUniverseIndexer('fields_small_ps_amftrue_pbftrue')
    # indexer = EnvPartitionedUniverseIndexer('viscore_fields_amftrue_pbffalse')
    # indexer.index()
    # print(indexer.count_possible_privs())

    indexer = EnvPartitionedUniverseIndexer('full_common_scoring_nd_amftrue_pbftrue')
    indexer.index()
    print(indexer.count_indexed_privs())