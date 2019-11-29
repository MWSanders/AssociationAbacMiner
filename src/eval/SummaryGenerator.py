import os
import json
from src.eval.PolicyEvaluator import ParallelUorHashGenerator

for filename in os.listdir('output'):
    policy = json.load(open(filename))

    print(filename)