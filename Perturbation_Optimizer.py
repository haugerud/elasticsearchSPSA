import random
from race_reader import race_reader
from Parameters import Elasticsearch_Parameters
import ESRally_Connector
import logging
import math
from Plotting import Plotting

class Optimizer(object):
    """docstring for Optimizer."""

    def __init__(self, Parameters_object):
        super(Optimizer, self).__init__()

        # define the elasticsearch ip and port to connect esrally
        self.connector = ESRally_Connector.ESRally_connector("128.39.120.25:9200")
        self.par = Parameters_object

        # define a plotting object
        self.plotting = Plotting()

        # creates a logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.file_handler = logging.FileHandler('tuning_process.log')
        self.formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def objective_function(self, indexing, response_time):
        self.plotting.add_latency_value(response_time)
        self.plotting.add_indexing_value(indexing)
        self.plotting.add_index_latency((response_time,indexing))
        return indexing/response_time, [response_time,indexing]


    def next_vector_value(self, minmax):
        i = 0
        limit = len(minmax)
        vector = [0 for i in range(limit)]

        # get scaling values in a list
        scale = [self.par.scale_index_refresh_interval,
                 self.par.scale_index_translog_flush_threshold_size]
        random_operator = [random.choice((-1, 1)) for _ in range(limit)]
        chance = [a*b for a, b in zip(scale, random_operator)]
        print("the X+ = ", chance)
        X_negative = [i * -1 for i in chance]  # get X- by multply -1 to X+
        print("the X- = ", X_negative)

        #####
        for i in range(limit):
            vector[i] = self.next(minmax[i], chance[i])

        return vector

    def next(self, current_par, chance):
        # increase or decrease value of a pramaeter
        if (((current_par[2] + chance) <= current_par[1]) or ((current_par[2] + chance) >= current_par[0])):
            return (current_par[2] + chance)


    def optimizer(self,current_par, max_iter):
        best_of_best = {}

        best = {}
        candidate = {}
        self.x_plus_minus_improvement = 0

        self.current_par = current_par
        self.logger.info('Set of Parameters = ' + str(current_par) )

        for i in range(max_iter):
            X_positive, X_negative = self.x_plus_minus(self.current_par)
            candidate['vector_positive'] = X_positive
            candidate['vector_negative'] = X_negative

            self.logger.info('X+ Parameters  '+ str(X_positive))
            self.logger.info('X- Parameters  '+ str(X_negative))

            print("Vector_Positive : ", str(candidate['vector_positive']))
            print("vector_negative : ", str(candidate['vector_negative']))

            # get objective function of the X+ and X-
            of_plus = self.takes_vector(candidate['vector_positive'])[0]
            self.plotting.add_dot_data((i+1,float(of_plus)))

            print("OF of X+ = ", of_plus)
            self.logger.info('Objective function of X+ = ' + str(of_plus))

            of_minus = self.takes_vector(candidate['vector_negative'])[0]
            self.plotting.add_dot_data((i+1,float(of_minus)))


            print("OF of X- = ", of_minus)
            self.logger.info('Objective function of X- = ' + str(of_minus))

            # get the improvement percentage of of+ and of-
            self.x_plus_minus_improvement = self.calculate_percentage_difference(float(of_plus), float(of_minus))

            print("calculate_percentage_difference : ", self.x_plus_minus_improvement)
            self.logger.info('calculate percentage difference = ' + str(self.x_plus_minus_improvement))
            # find which of (x+ x-) is best
            self.logger.info('choosing which direction to go  x+ or x- ')

            if (of_plus > of_minus):
                best['cost'] = of_plus
                best['vector'] = candidate['vector_positive']
                self.logger.info('X+ is better than X- with objective_function = '+ str(best['cost']))

                # add line plotting to the graph
                self.plotting.add_line_data((i+1,float(of_plus)))

                self.update_parameters(best)
                self.logger.info('setting new parameters value based on X+ which is '+ str(best['vector']))
            else:
                best['cost'] = of_minus
                best['vector'] = candidate['vector_negative']
                self.logger.info('X- is better than X+ with objective_function = '+ str(best['cost']))

                # add line plotting to the graph
                self.plotting.add_line_data((i+1,float(of_minus)))


                self.update_parameters(best)
                self.logger.info('setting new parameters value based on X- which is '+ str(best['vector']))

            if not best_of_best or best['cost'] > best_of_best['cost']:
                best_of_best['cost'] = best.get('cost')
                best_of_best['vector'] = best.get('vector')
                self.logger.info('updating best_of_best dic with value :::: '+ str(best_of_best))

            #setting the new list of [min,max,current]
            self.current_par = [self.par.minmax_index_refresh_interval_current(self.par.index_refresh_interval),
            self.par.minmax_index_translog_flush_threshold_size_current(self.par.index_translog_flush_threshold_size),self.par.minmax_translog_sync_interval_current(self.par.translog_sync_interval),
            self.par.minmax_recovery_max_bytes_per_sec_current(self.par.recovery_max_bytes_per_sec)]

            # update the scaling values by multiplying to the percentage difference
            self.logger.info('Updating the step size')
            self.update_step_size()
            #self.update_scale()

            scale = [self.par.scale_index_refresh_interval,self.par.scale_index_translog_flush_threshold_size,self.par.scale_translog_sync_interval,
            self.par.recovery_max_bytes_per_sec]
            print("after updating scaling values : ", scale)
            self.logger.info('The new scaling values for each parameter in order '+ str(scale))

        self.logger.info('returning Best  '+ str(best_of_best))
        self.logger.info("Line_data '{0}' and dots_data '{1}'".format(str(self.plotting.get_line_dots()[0]),str(self.plotting.get_line_dots()[1])))
        self.logger.info("indexing data and latency" + str(self.plotting.get_indexing_value())+" ########## "+str(self.plotting.get_latency_value()))
        self.logger.info("indexing data and latency '{0}'".format(str(self.plotting.get_latency_value())))
        return best_of_best

    def x_plus_minus(self, minmax):
        i = 0
        limit = len(minmax)
        vector_x_plus = [0 for i in range(limit)]
        vector_x_minus = [0 for i in range(limit)]
        # get scaling values in a list
        scale = [self.par.scale_index_refresh_interval,self.par.scale_index_translog_flush_threshold_size,self.par.scale_translog_sync_interval,self.par.scale_recovery_max_bytes_per_sec]
        # make a random + or - to each parameter
        random_operator = [random.choice((-1, 1)) for _ in range(limit)]
        X_positive = [a*b for a, b in zip(scale, random_operator)]
        # get X- by multply -1 to X+
        X_negative = [i * -1 for i in X_positive]

        for i in range(limit):
            vector_x_plus[i] = self.next(minmax[i], X_positive[i])
            vector_x_minus[i] = self.next(minmax[i], X_negative[i])

        # return x+ and x- in a list
        return [vector_x_plus, vector_x_minus]

    def calculate_percentage_difference(self, v1, v2):
        return ((abs(v1 - v2) / ((v1+v2)/2)) * (100.0))/100

    def update_parameters(self,list):
        self.par.index_refresh_interval = list['vector'][0]
        self.par.index_translog_flush_threshold_size = list['vector'][1]
        self.par.translog_sync_interval = list['vector'][2]
        self.par.recovery_max_bytes_per_sec = list['vector'][3]

    def update_scale(self):
        ##### refresh interval
        # get the expected next par value and check if it's within minmax range
        minmax_current_rf = self.par.return_minmax_index_refresh_interval_current()
        rf_tmp_plus = math.ceil(((self.par.scale_index_refresh_interval * self.x_plus_minus_improvement)+self.par.scale_index_refresh_interval) + minmax_current_rf[2])
        rf_tmp_minus= math.ceil((-1*(self.par.scale_index_refresh_interval * self.x_plus_minus_improvement))+self.par.scale_index_refresh_interval + minmax_current_rf[2])
        rf_mx = minmax_current_rf[1]
        rf_min = minmax_current_rf[0]

        self.logger.info('update_scale() minmax_current_rf,rf_tmp_plus ,rf_tmp_minus '+ str(minmax_current_rf)+ str(rf_tmp_plus)+ str(rf_tmp_minus))
        if rf_min <= rf_tmp_plus <= rf_mx and rf_min <= rf_tmp_minus <= rf_mx :
            self.par.set_scale_index_refresh_interval(math.ceil((self.par.scale_index_refresh_interval * self.x_plus_minus_improvement)+self.par.scale_index_refresh_interval))
        else:
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for RI ' + str(self.par.scale_index_refresh_interval))

        ##### threshold size
        minmax_current_ts = self.par.return_minmax_index_translog_flush_threshold_size_current()
        ts_tmp_plus = math.ceil((self.par.scale_index_translog_flush_threshold_size * self.x_plus_minus_improvement+self.par.scale_index_translog_flush_threshold_size)+ minmax_current_ts[2])
        ts_tmp_minus= math.ceil(((self.par.scale_index_translog_flush_threshold_size * self.x_plus_minus_improvement)*(-1))+self.par.scale_index_translog_flush_threshold_size + minmax_current_ts[2])
        ts_mx = minmax_current_ts[1]
        ts_min = minmax_current_ts[0]

        if ts_min <= ts_tmp_plus <= ts_mx and ts_min <= ts_tmp_minus <= ts_mx:
            self.par.set_scale_index_translog_flush_threshold_size(math.ceil((self.par.scale_index_translog_flush_threshold_size *self.x_plus_minus_improvement))+self.par.scale_index_translog_flush_threshold_size)
        else:
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for ' + str(self.par.scale_index_translog_flush_threshold_size))


        ##### recovery_max_bytes_per_sec
        minmax_current_rmb = self.par.return_minmax_recovery_max_bytes_per_sec_current()
        rmb_tmp_plus = math.ceil((self.par.scale_recovery_max_bytes_per_sec * self.x_plus_minus_improvement+self.par.scale_recovery_max_bytes_per_sec)+ minmax_current_rmb[2])
        rmb_tmp_minus= math.ceil(((self.par.scale_recovery_max_bytes_per_sec * self.x_plus_minus_improvement)*(-1))+self.par.scale_recovery_max_bytes_per_sec + minmax_current_rmb[2])
        rmb_mx = minmax_current_rmb[1]
        rmb_min = minmax_current_rmb[0]

        if rmb_min <= rmb_tmp_plus <= rmb_mx and rmb_min <= rmb_tmp_minus <= rmb_mx:
            self.par.set_scale_recovery_max_bytes_per_sec(math.ceil((self.par.scale_recovery_max_bytes_per_sec *self.x_plus_minus_improvement))+self.par.scale_recovery_max_bytes_per_sec)
        else:
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for ' + str(self.par.scale_recovery_max_bytes_per_sec))




    def update_step_size(self):
        ##### refresh interval
        # get the expected next par value and check if it's within minmax range
        minmax_current_rf = self.par.return_minmax_index_refresh_interval_current()
        rf_tmp_plus = math.ceil(self.par.calculate_step_size_refresh_interval(self.x_plus_minus_improvement)) + minmax_current_rf[2]
        rf_tmp_minus= math.ceil((-1*(self.par.calculate_step_size_refresh_interval(self.x_plus_minus_improvement))) + minmax_current_rf[2])
        rf_mx = minmax_current_rf[1]
        rf_min = minmax_current_rf[0]

        self.logger.info(' update_step_size() minmax_current_rf,rf_tmp_plus ,rf_tmp_minus '+ str(minmax_current_rf)+ str(rf_tmp_plus)+","+ str(rf_tmp_minus))
        if rf_min <= rf_tmp_plus <= rf_mx and rf_min <= rf_tmp_minus <= rf_mx :
            self.par.set_scale_index_refresh_interval(math.ceil(self.par.calculate_step_size_refresh_interval(self.x_plus_minus_improvement)))
        else:
            self.par.set_scale_index_refresh_interval(math.ceil(self.par.scale_index_refresh_interval*0.01))
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for RI ' + str(self.par.scale_index_refresh_interval))

        ##### threshold size
        minmax_current_ts = self.par.return_minmax_index_translog_flush_threshold_size_current()
        ts_tmp_plus = math.ceil(self.par.calculate_step_size_threshold_size(self.x_plus_minus_improvement))+ minmax_current_ts[2]
        ts_tmp_minus= math.ceil(-1*(self.par.calculate_step_size_threshold_size(self.x_plus_minus_improvement)) + minmax_current_ts[2])
        ts_mx = minmax_current_ts[1]
        ts_min = minmax_current_ts[0]

        if ts_min <= ts_tmp_plus <= ts_mx and ts_min <= ts_tmp_minus <= ts_mx:
            self.par.set_scale_index_translog_flush_threshold_size(math.ceil(self.par.calculate_step_size_threshold_size(self.x_plus_minus_improvement)))
        else:
            self.par.set_scale_index_translog_flush_threshold_size(math.ceil(self.par.scale_index_translog_flush_threshold_size*0.01))
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for TS ' + str(self.par.scale_index_translog_flush_threshold_size))


       ##### translog_sync_interval
        minmax_current_si = self.par.return_minmax_translog_sync_interval_current()
        si_tmp_plus = math.ceil(self.par.calculate_step_size_translog_sync_interval(self.x_plus_minus_improvement))+ minmax_current_si[2]
        si_tmp_minus= math.ceil(-1*(self.par.calculate_step_size_translog_sync_interval(self.x_plus_minus_improvement)) + minmax_current_si[2])
        si_mx = minmax_current_si[1]
        si_min = minmax_current_si[0]

        if si_min <= si_tmp_plus <= si_mx and si_min <= si_tmp_minus <= si_mx:
            self.par.set_scale_translog_sync_interval(math.ceil(self.par.calculate_step_size_translog_sync_interval(self.x_plus_minus_improvement)))
        else:
            self.par.set_scale_translog_sync_interval(math.ceil(self.par.scale_translog_sync_interval*0.01))
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for SI' + str(self.par.scale_translog_sync_interval))

        ##### recovery_max_bytes_per_sec
        minmax_current_rmb = self.par.return_minmax_recovery_max_bytes_per_sec_current()
        rmb_tmp_plus = math.ceil(self.par.calculate_step_size_recovery_max_bytes_per_sec(self.x_plus_minus_improvement))+ minmax_current_si[2]
        rmb_tmp_minus= math.ceil(-1*(self.par.calculate_step_size_recovery_max_bytes_per_sec(self.x_plus_minus_improvement)) + minmax_current_si[2])
        rmb_mx = minmax_current_rmb[1]
        rmb_min = minmax_current_rmb[0]

        if rmb_min <= rmb_tmp_plus <= rmb_mx and rmb_min <= rmb_tmp_minus <= rmb_mx:
            self.par.set_scale_recovery_max_bytes_per_sec(math.ceil(self.par.calculate_step_size_recovery_max_bytes_per_sec(self.x_plus_minus_improvement)))
        else:
            self.par.set_scale_recovery_max_bytes_per_sec(math.ceil(self.par.scale_recovery_max_bytes_per_sec*0.1))
            self.logger.info('new scale value exceeds the minmax range. The same scaling value will be used for RMB' + str(self.par.scale_recovery_max_bytes_per_sec))


    def takes_vector(self, candidate):
        # take a set of parameters and perofrm the elasticsearch update and then return the value of the objective function
        self.par.index_refresh_interval = candidate[0]
        self.par.index_translog_flush_threshold_size = candidate[1]
        self.par.translog_sync_interval = candidate[2]
        self.par.recovery_max_bytes_per_sec = candidate[3]
        self.connector.update_node_settings(self.par.index_refresh_interval_string(), self.par.index_translog_flush_threshold_size_string(),self.par.index_translog_flush_threshold_size_string(),self.par.recovery_max_bytes_per_sec_string())
        self.logger.info('Updating Elasticsearch settings')
        self.myRace = race_reader(self.connector.get_race_json())
        self.logger.info('Start ESRally benchamrking ')
        return self.objective_function(self.myRace.index_throughput_mean(), self.myRace.ops_latency_average_mean())


    def make_graph(self):
        return self.plotting.plot_chart()
