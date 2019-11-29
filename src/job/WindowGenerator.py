from src.config import  config
import dateutil.parser
from datetime import datetime, timedelta
import copy, json

class WindowGenerator(object):
    def __init__(self, job_config):
        self.obs_days = job_config['obs_days']
        self.opr_days = job_config['opr_days']
        self.calendar_start = dateutil.parser.parse(job_config['calendar_start'])
        self.calendar_end = dateutil.parser.parse(job_config['calendar_end'])

    def generate_windows(self):
        results = []
        current_start_date = self.calendar_start
        obs_end = current_start_date + timedelta(days=self.obs_days)
        opr_end = obs_end + timedelta(self.opr_days)
        while opr_end < self.calendar_end:
            window = Window(current_start_date, obs_end, opr_end)
            current_start_date += timedelta(days=1)
            obs_end += timedelta(days=1)
            opr_end += timedelta(days=1)
            results.append(window)
        return results


class Window(object):
    def __init__(self, obs_start, obs_end_opr_start, opr_end):
        self.obs_start = obs_start
        self.obs_end_opr_start = obs_end_opr_start
        self.opr_end = opr_end

    def update_obs_query_with_window_bounds(self, query):
        opr_source_query = copy.deepcopy(query)
        opr_source_query['eventTime']['$gte'] = self.obs_start
        opr_source_query['eventTime']['$lte'] = self.obs_end_opr_start
        return opr_source_query

    def update_opr_query_with_window_bounds(self, query):
        opr_source_query = copy.deepcopy(query)
        opr_source_query['eventTime']['$gte'] = self.obs_end_opr_start
        opr_source_query['eventTime']['$lte'] = self.opr_end
        return opr_source_query

    def update_obs_opr_query_with_window_bounds(self, query):
        obs_opr_source_query = copy.deepcopy(query)
        obs_opr_source_query['eventTime']['$gte'] = self.obs_start
        obs_opr_source_query['eventTime']['$lte'] = self.opr_end
        return obs_opr_source_query

    def __str__(self):
        obj_as_dict = self.__dict__
        return json.dumps(obj_as_dict, sort_keys=True, default=str)

if __name__ == "__main__":
    window_generator = WindowGenerator()
    window_generator.generate_windows()