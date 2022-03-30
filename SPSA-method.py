def x(self,current_par, max_iter):
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


            # get objective function of the X+ and X-
            of_plus = self.takes_vector(candidate['vector_positive'])[0]
            self.plotting.add_dot_data((i+1,float(of_plus)))



            of_minus = self.takes_vector(candidate['vector_negative'])[0]



            # get the improvement percentage of of+ and of-
            self.x_plus_minus_improvement = self.calculate_percentage_difference(float(of_plus), float(of_minus))


            # find which of (x+ x-) is best
            self.logger.info('choosing which direction to go  x+ or x- ')

            if (of_plus > of_minus):
                best['cost'] = of_plus
                best['vector'] = candidate['vector_positive']
                self.logger.info('X+ is better than X- with objective_function = '+ str(best['cost']))


                self.update_parameters(best)
     
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


            #setting the new list of [min,max,current]
            self.current_par = [self.par.minmax_index_refresh_interval_current(self.par.index_refresh_interval),self.par.minmax_index_translog_flush_threshold_size_current(self.par.index_translog_flush_threshold_size),self.par.minmax_translog_sync_interval_current(self.par.translog_sync_interval)]

            # update the scaling values by multiplying to the percentage difference
            self.update_step_size()


            scale = [self.par.scale_index_refresh_interval,self.par.scale_index_translog_flush_threshold_size]

        return best_of_best
