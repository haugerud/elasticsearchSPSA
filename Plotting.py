import leather


class Plotting(object):

    def __init__(self):
        self.line_data = []
        self.dot_data = []
        self.latency = []
        self.indexing = []
        self.latency_index = []
        self.chart = leather.Chart('Results')

    def add_line_data(self,list):
        self.line_data.append(list)

    def add_dot_data(self,list):
        self.dot_data.append(list)

    def plot_chart(self):
        self.chart.add_line(self.line_data)
        self.chart.add_dots(self.dot_data)
        self.chart.to_svg('result.svg')
        #/home/elasticsearch

    def get_line_dots(self):
        return self.line_data, self.dot_data

    def add_latency_value(self,value):
        self.latency.append(value)

    def add_indexing_value(self,value):
        self.indexing.append(value)

    def get_latency_value(self):
        return self.latency

    def get_indexing_value(self):
        return self.indexing

    def add_index_latency(self,list):
        self.latency_index.append(list)

    def get_index_latency_value(self):
        return self.latency_index
