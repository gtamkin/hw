#!/usr/bin/env python
'''
Created on January 25, 2017

@author: gtamkin
'''
import xml.etree.ElementTree as ET
import shutil
import time
import sys
import os
import multiprocessing
from multiprocessing import Process, Queue

from CDSLibrary import CDSApi
cds_lib = CDSApi()
from uqp_ensemble_input import UQPInput
uqp_exp = UQPInput()

# climatology parameters
catalog = uqp_exp.getInput()
climatology_type = uqp_exp.getClimatologyType()
destination = uqp_exp.getDestination()
regrid_dictionary = uqp_exp.getRegridDictionary()

# regridding parameters
regridder = uqp_exp.getRegridder()
regrid_variable = uqp_exp.getRegridVariable()
regrid_nx = uqp_exp.getRegridX()
regrid_ny = uqp_exp.getRegridY()
regrid_parms = "&regridder=" + regridder + "&regrid_variable" + regrid_variable + "&nx=" + regrid_nx + "&ny" + regrid_ny

# logger and internal catologs
logger = cds_lib.getLogger()
sessionCatalog = dict({})
diffCatalog = dict({})
collectionCatalog = dict({})
operationCatalog = dict({})
threadCatalog = dict({})
diffThreadCatalog = dict({})

def regrid(collectionCatalog, sessionCatalog, regridCatalog, operation, service, key, filename, logger, cds_lib, start_time): 
    sessionId = sessionCatalog[key]
    logger.debug("Polling for: key[" + key + "] sessionId[" + sessionId + "] filename [" + filename + "] + operation [" + operation + "]")
    response = cds_lib.poll(service, sessionId, filename, cds_lib.cds_ws.config)
    sessionStatus = cds_lib.getElement(response, "sessionStatus")
    if sessionStatus == 'Completed':    
        # 2)  new 'execute' regrid service_request that:
        #    a) invokes regridding using the Earth System Modeling Framework (ESMF) regridding utility (Python) 
        #    b) invokes the NCO (netCDF Operators) 'ncks' utility to convert the regridded output to netCDF3 CLASSIC storage format. 
        #     NCO is a collection of utilities to manipulate and analyze netCDF files,            
        
        # prepare parameters
        ds = regrid_dictionary
        ds['session_id'] = sessionId
        ds['collection'] = collectionCatalog[sessionId]
        service = ds['service']
        parms = cds_lib.encode(ds);
        
        elapsed_time = "%0.2f" % (time.time() - start_time)
        logger.debug("Climatology completed for: operation[" + operation + "], key[" + key + "] in " + elapsed_time + " seconds")

        if operation == 'avg':
		# perform regridding
		logger.debug("Executing regridding for: key[" + key + "], climatology sessionId [" + sessionId + "]")
		regrid_time = time.time()
		response = cds_lib.regrid(service, parms, "None")

		# save regrid session id in regridCatalog for later use in ensemble average
		elapsed_time = "%0.2f" % (time.time() - regrid_time)
		regridSessionId = cds_lib.getElement(response, "sessionId")
		regridCatalog[key] = regridSessionId
		logger.debug("Regridding complete for: key[" + key + "], regrid sessionId [" + regridCatalog[key] + "] in " + elapsed_time + " seconds")
    else:
        detail = cds_lib.getElement(response, "sessionStatusDetail")
        logger.error(sessionStatus + " for: " + sessionId + ".  Detail: " + detail) 

def getResult(service, sessionId, filename, destination, logger, cds_lib): 
    logger.debug("Polling for: sessionId[" + sessionId +"] filename [" + filename + "]")
    response = cds_lib.poll(service, sessionId, filename, cds_lib.cds_ws.config)
    sessionStatus = cds_lib.getElement(response, "sessionStatus")
    if sessionStatus == 'Completed':    
        cds_lib.downloadResult(sessionStatus, service, sessionId, destination, filename)
        logger.debug("Result retrieved for: " + destination + "/" + filename)
    else:
        detail = cds_lib.getElement(response, "sessionStatusDetail")
        logger.error(sessionStatus + " for: " + sessionId + ".  Detail: " + detail) 

def getResults(catalog, destination, prefix):
    keylist = catalog.keys()
    keylist.sort()
    if prefix:
        prefix = prefix + "_"
    for key in keylist:
        filename = prefix + key +  "_" + catalog[key] + ".nc"
        getResult("RES", catalog[key], filename, destination, logger, cds_lib)

def waitOnThreads(catalog, event, start_time, logger): 
     # wait for the threads to complete
    keylist = catalog.keys()
    keylist.sort()
    for key in keylist:
        p = catalog[key]
        p.join()
    calculateElapsedTimeForEvent(event, start_time, logger) 

def calculateElapsedTimeForEvent(event, start_time, logger): 
     elapsed_time = "%0.2f" % (time.time() - start_time)
     logger.debug("-----Total seconds wall clock time for [" + event + "] = " + elapsed_time)
     logger.debug(" ") 

#operations = ['avg']
#operations = ['std']
operations = ['avg','std']
#operations = ['avg','std','obs']

class UserApp(object):
    
    def main(self, yearly_datasets, long_term_datasets, destination): 

        '''
        This method calculates an ensemble across the specified spatial and temporal extents.
        '''

        try:     
            
            # climatology parameters
            catalog = uqp_exp.getInput()
            climatology_type = uqp_exp.getClimatologyType()
            destination = uqp_exp.getDestination()
            regrid_dictionary = uqp_exp.getRegridDictionary()

            # regridding parameters
            regridder = uqp_exp.getRegridder()
            regrid_variable = uqp_exp.getRegridVariable()
            regrid_nx = uqp_exp.getRegridX()
            regrid_ny = uqp_exp.getRegridY()
            regrid_parms = "&regridder=" + regridder + "&regrid_variable" + regrid_variable + "&nx=" + regrid_nx + "&ny" + regrid_ny

            # logger and internal catologs
            logger = cds_lib.getLogger()
            sessionCatalog = dict({})
            diffCatalog = dict({})
            collectionCatalog = dict({})
            operationCatalog = dict({})
            threadCatalog = dict({})
            diffThreadCatalog = dict({})

            # record start time
            start_time = time.time()

            # parse dictionaries and launch climatologies
#            datasets = yearly_datasets + long_term_datasets
            datasets = yearly_datasets
            for dataset in datasets:
               for operation in operations:
                   currentDictionary = dataset + "_dictionary"
                   ds = catalog[currentDictionary]
                   ds['climatology_type'] = climatology_type
                   ds['operation'] = operation

		   # 1) classic CDS 'order' that retrieves data from RES service via Hadoop/MR processing
		   sessionId = cds_lib.climatology(ds['service'], ds['service_request'], ds)

		   # save the climatology session id & associated data collection
		   sessionCatalog[dataset + "_" + operation + "_sessionId"] = sessionId
		   operationCatalog[dataset + "_" + operation + "_sessionId"] = operation
		   collectionCatalog[sessionId] = ds['collection']
		   logger.debug("Ordered climatology for: [" + currentDictionary + "], climatology sessionId [" + sessionId + "], operation [" + operation + "]")

            # create output directory based on last sessionId
            destination = destination + "/" + sessionId
            if (os.path.isdir(destination) == False):
                os.makedirs(destination)

            # create and start the climatology thread pool
            keylist = sessionCatalog.keys()
            keylist.sort()
            mgr = multiprocessing.Manager()
            regridCatalog = mgr.dict()            
            for key in keylist:
                filename = sessionCatalog[key] + ".nc"
		operation = operationCatalog[key]
                climatology_time = time.time()
                p = Process(target=regrid, args=(collectionCatalog, sessionCatalog, regridCatalog, operation, "RES", key, filename, logger, cds_lib, climatology_time))
                p.start()
                threadCatalog[sessionCatalog[key]] = p
     
             # wait for the climatology/regridding threads to complete and log performance
            waitOnThreads(threadCatalog,"climatologies & regridding", start_time, logger) 

#            sys.exit("bye");

            # now that regridding is complete, calculate the long-term ensemble average based on regridded long-term climatologies
#            datasets = long_term_datasets
            datasets = yearly_datasets
            ensemble_list = ""
            for dataset in datasets:
                # retrieve long-term regridded session id
#                ensemble_list = ensemble_list + regridCatalog[dataset + "_" + operation + "_sessionId"] + "|"
                ensemble_list = ensemble_list + regridCatalog[dataset + "_avg_sessionId"] + "|"
            ensemble_list = ensemble_list[:-1]
            logger.debug("Ensemble average list [" + ensemble_list+ "]")

            # 3)  new 'execute' ensemble_avg service_request that:
            #    a) invokes the NCO 'nces' (netCDF Ensemble Averager), to average a set of files or groups, weighting each file or group evenly.
            ed = dict({})
            ed['session_list'] = ensemble_list
            parms = cds_lib.encode(ed);
            ensemble_time = time.time()
            ensemble_response = cds_lib.ensemble_avg("RES", parms, "None")
            regridded_long_term_avg_ensemble_sessionId = cds_lib.getElement(ensemble_response, "sessionId")

            # determine elapsed time for ensemble average
            logger.debug("Ensemble Avg finished for: [" + regridded_long_term_avg_ensemble_sessionId + "]")
            calculateElapsedTimeForEvent("ensemble average", ensemble_time, logger)
                                    
            # download climatologies
            logger.debug("downloads started...")
            download_time = time.time()
            getResults(sessionCatalog, destination, "")
                        
            # download regridded climatologies
            getResults(regridCatalog, destination, "regridded")       
                 
            getResult("RES", regridded_long_term_avg_ensemble_sessionId, "long_term_avg_ensemble_" + regridded_long_term_avg_ensemble_sessionId + ".nc", destination, logger, cds_lib)

            calculateElapsedTimeForEvent("downloads", download_time, logger)         
   
        except Exception, ex:
            logger.error("An error occurred: {0}".format(ex))

        # determine elapsed time for ensemble average
        calculateElapsedTimeForEvent("uqp ensemble use case", start_time, logger)

#            sys.exit("bye");

    if __name__ == '__main__':   

        # specify datasets for ensemble
        yearly_datasets = [
                 'climatology_yearly_cfsr',
                 'climatology_yearly_merra'] 

        long_term_datasets = [
                 'climatology_long_term_cfsr',
                 'climatology_long_term_merra'] 
        
        yearly_datasets_ = [
                 'climatology_yearly_avg_cfsr',
                 'climatology_yearly_avg_ecmwf',
                 'climatology_yearly_avg_merra'] 

        long_term_datasets_ = [
                 'climatology_long_term_avg_cfsr',
                 'climatology_long_term_avg_ecmwf',
                 'climatology_long_term_avg_merra'] 

        main(object, yearly_datasets, long_term_datasets, destination)

