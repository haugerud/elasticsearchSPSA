import json

class race_reader(object):
    """
     Reading json files to return certain values
    """
    def __init__(self, json_file):
        with open(json_file) as js:
            self.json_file = json.load(js)

    def index_throughput_mean(self):
        return float(json.dumps(self.json_file["results"]["op_metrics"][0]["throughput"]["mean"]))

    def ops_latency_average_mean(self):
        #since the number of operations is the same for each type we take the average
        ops_len = len(self.json_file["results"]["op_metrics"])
        res = 0
        for i in range(1, ops_len):
            res = + float(self.json_file["results"]["op_metrics"][i]["latency"]["mean"])
        return res / ops_len
