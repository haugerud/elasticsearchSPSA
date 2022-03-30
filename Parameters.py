import math

class Elasticsearch_Parameters:
    def __init__(self,_index_refresh_interval=10,_index_translog_flush_threshold_size=300,_translog_sync_interval=100,_recovery_max_bytes_per_sec=50):
          self._index_refresh_interval = _index_refresh_interval
          self._index_translog_flush_threshold_size = _index_translog_flush_threshold_size
          self._translog_sync_interval = _translog_sync_interval
          self._recovery_max_bytes_per_sec = _recovery_max_bytes_per_sec
     # using property decorator
     # a getter function

################### index.referesh_interval
    @property
    def index_refresh_interval(self):
         return self._index_refresh_interval

    @index_refresh_interval.setter
    def index_refresh_interval(self, value):
        if (value > self.minmax_index_refresh_interval()[1]):
            self._index_refresh_interval = self.minmax_index_refresh_interval()[1]
        if (value < self.minmax_index_refresh_interval()[0]):
            self._index_refresh_interval = self.minmax_index_refresh_interval()[0]
        else:
            self._index_refresh_interval = value

    def index_refresh_interval_string(self):
         return (str(self._index_refresh_interval) + "s")

    def minmax_index_refresh_interval(self):
        return [1,8000]

    def minmax_index_refresh_interval_current(self,current):
        self._index_refresh_interval = current
        return [1,8000,self._index_refresh_interval]

    def return_minmax_index_refresh_interval_current(self,):
        return [1,8000,self._index_refresh_interval]

    def set_scale_index_refresh_interval(self,scale):
        self.scale_index_refresh_interval = scale
    def scale_index_refresh_interval(self):
        return self.scale_index_refresh_interval

    def scale_minmax_index_refresh_interval(self):
        minmax = self.minmax_index_refresh_interval()
        min = math.ceil((minmax[1]-minmax[0] )/100)
        max = math.ceil((minmax[1]-minmax[0] )/10)
        return [min,max]

    def calculate_step_size_refresh_interval(self,improvement_percentage):
        res = self.scale_minmax_index_refresh_interval()[0] * (1 + improvement_percentage)
        if res < self.scale_minmax_index_refresh_interval()[1]:
            return res
        else:
            return self.scale_minmax_index_refresh_interval()[1]


##################### index.flush_threshold_size
    @property
    def index_translog_flush_threshold_size(self):
         return self._index_translog_flush_threshold_size

    @index_translog_flush_threshold_size.setter
    def index_translog_flush_threshold_size(self, value):
        if (value > self.minmax_index_translog_flush_threshold_size()[1]):
            self._index_translog_flush_threshold_size = self.minmax_index_translog_flush_threshold_size()[1]
        if (value < self.minmax_index_translog_flush_threshold_size()[0]):
            self._index_translog_flush_threshold_size = self.minmax_index_translog_flush_threshold_size()[0]
        else:
            self._index_translog_flush_threshold_size = value

    def index_translog_flush_threshold_size_string(self):
         return (str(self._index_translog_flush_threshold_size) + "mb")

    def minmax_index_translog_flush_threshold_size(self):
         return [112,10000]

    def minmax_index_translog_flush_threshold_size_current(self,current):
        self._index_translog_flush_threshold_size = current
        return [112,10000,self._index_translog_flush_threshold_size]

    def return_minmax_index_translog_flush_threshold_size_current(self,):
        return [112,10000,self._index_translog_flush_threshold_size]

    def set_scale_index_translog_flush_threshold_size(self,scale):
        self.scale_index_translog_flush_threshold_size = scale
    def scale_index_translog_flush_threshold_size(self):
        return self.scale_index_translog_flush_threshold_size

    def scale_minmax_index_translog_flush_threshold_size(self):
        minmax = self.minmax_index_translog_flush_threshold_size()
        min = math.ceil((minmax[1]-minmax[0] )/100)
        max = math.ceil((minmax[1]-minmax[0] )/10)
        return [min,max]

    def calculate_step_size_threshold_size(self,improvement_percentage):
        res = self.scale_minmax_index_translog_flush_threshold_size()[0] * (1 + improvement_percentage)
        if res < self.scale_minmax_index_translog_flush_threshold_size()[1]:
            return res
        else:
            return self.scale_minmax_index_translog_flush_threshold_size()[1]


################################ translog_sync_interval
    @property
    def translog_sync_interval(self):
         return self._translog_sync_interval

    @translog_sync_interval.setter
    def translog_sync_interval(self, value):
        if (value > self.minmax_translog_sync_interval()[1]):
            self._translog_sync_interval = self.minmax_translog_sync_interval()[1]
        if (value < self.minmax_translog_sync_interval()[0]):
            self._translog_sync_interval = self.minmax_translog_sync_interval()[0]
        else:
            self._translog_sync_interval = value

    def translog_sync_interval_string(self):
         return (str(self._translog_sync_interval) + "s")

    def minmax_translog_sync_interval(self):
         return [1,10000]

    def minmax_translog_sync_interval_current(self,current):
        self._translog_sync_interval = current
        return [1,10000,self._translog_sync_interval]

    def return_minmax_translog_sync_interval_current(self,):
        return [1,10000,self._translog_sync_interval]

    def set_scale_translog_sync_interval(self,scale):
        self.scale_translog_sync_interval = scale
    def scale_translog_sync_interval(self):
        return self.scale_translog_sync_interval

    def scale_minmax_translog_sync_interval(self):
        minmax = self.minmax_translog_sync_interval()
        min = math.ceil((minmax[1]-minmax[0] )/100)
        max = math.ceil((minmax[1]-minmax[0] )/10)
        return [min,max]

    def calculate_step_size_translog_sync_interval(self,improvement_percentage):
        res = self.scale_minmax_translog_sync_interval()[0] * (1 + improvement_percentage)
        if res < self.scale_minmax_translog_sync_interval()[1]:
            return res
        else:
            return self.scale_minmax_index_translog_sync_interval()[1]

##############################indices.recovery.max_bytes_per_sec

    @property
    def recovery_max_bytes_per_sec(self):
         return self._recovery_max_bytes_per_sec

    @recovery_max_bytes_per_sec.setter
    def recovery_max_bytes_per_sec(self, value):
        if (value > self.minmax_recovery_max_bytes_per_sec()[1]):
            self._recovery_max_bytes_per_sec = self.minmax_recovery_max_bytes_per_sec()[1]
        if (value < self.minmax_recovery_max_bytes_per_sec()[0]):
            self._recovery_max_bytes_per_sec = self.minmax_recovery_max_bytes_per_sec()[0]
        else:
            self._recovery_max_bytes_per_sec = value

    def recovery_max_bytes_per_sec_string(self):
         return (str(self._recovery_max_bytes_per_sec) + "mb")

    def minmax_recovery_max_bytes_per_sec(self):
         return [50,10000]

    def minmax_recovery_max_bytes_per_sec_current(self,current):
        self._recovery_max_bytes_per_sec = current
        return [50,10000,self._recovery_max_bytes_per_sec]

    def return_minmax_recovery_max_bytes_per_sec_current(self):
        return [50,10000,self._recovery_max_bytes_per_sec]

    def set_scale_recovery_max_bytes_per_sec(self,scale):
        self.scale_recovery_max_bytes_per_sec = scale

    def scale_recovery_max_bytes_per_sec(self):
        return self.scale_recovery_max_bytes_per_sec

    def scale_minmax_recovery_max_bytes_per_sec(self):
        minmax = self.minmax_recovery_max_bytes_per_sec()
        min = math.ceil((minmax[1]-minmax[0] )/100)
        max = math.ceil((minmax[1]-minmax[0] )/10)
        return [min,max]

    def calculate_step_size_recovery_max_bytes_per_sec(self,improvement_percentage):
        res = self.scale_minmax_recovery_max_bytes_per_sec()[0] * (1 + improvement_percentage)
        if res < self.scale_minmax_recovery_max_bytes_per_sec()[1]:
            return res
        else:
            return self.scale_minmax_recovery_max_bytes_per_sec()[1]
