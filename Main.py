import ESRally_Connector
import race_reader
from perturbation_optimizer import Optimizer
from Parameters import Elasticsearch_Parameters


def main():
    #get init parameters
    par=Elasticsearch_Parameters(100,1500,100,7000)
    ri = par.index_refresh_interval
    fs = par.index_translog_flush_threshold_size
    si = par.translog_sync_interval
    mbr = par.recovery_max_bytes_per_sec
    # seting the scaling value of each parameter
    par.set_scale_index_refresh_interval(par.scale_minmax_index_refresh_interval()[0])
    par.set_scale_index_translog_flush_threshold_size(par.scale_minmax_index_translog_flush_threshold_size()[0])
    par.set_scale_translog_sync_interval(par.scale_minmax_translog_sync_interval()[0])
    par.set_scale_recovery_max_bytes_per_sec(par.scale_minmax_recovery_max_bytes_per_sec()[0])

    current_par = [par.minmax_index_refresh_interval_current(ri),par.minmax_index_translog_flush_threshold_size_current(fs),par.minmax_translog_sync_interval_current(si),par.minmax_recovery_max_bytes_per_sec_current(mbr)]




    search_space = current_par
    max_iter = 15

	# execute the algorithm
    best = Optimizer(par)
    #best = best.search(search_space ,max_iter)
    best = best.x(search_space ,max_iter)
    #Updating the settings from the best results
    self.connector.update_node_settings(best['cost'][0],best['cost'][1],best['cost'][2],best['cost'][3])
    best.make_graph()
    print("Done. Best Solution: cost = " + str(best['cost']) + ", v = " + str(best['vector']))


if __name__ == '__main__':
    main()
