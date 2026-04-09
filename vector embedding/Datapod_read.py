import os
import shutil
import mysql.connector
import requests
import pandas as pd
import json

import os
import logging
from ftplib import FTP
import os
import glob
import json
# from variable import *
import os
import sys
from time import perf_counter
import pandas as pd


import os
import logging
import getpass
import paramiko
from stat import S_ISDIR

print("Inside python script ")
# argument = None
# global argument
'''if len(sys.argv)>1:
    #global argument
    argument = sys.argv[1]
    argument1 = open('argument.pkl', 'wb')
    pickle.dump(argument, argument1)
    argument1.close()'''

if len(sys.argv) > 1 and sys.argv[1] != 'predictApiHostname':

    sourceDbName, activation, lossFunction, layerNames, dbName, targetPath, operation, password, url, port, special_space_replacer, \
        targetDbName, targetPort, userName, targetHostName, inputConfigFilePath, optimizationAlgo, sourceHostName, sourceDsType, \
        modelFilePath, featureWeightPath, sourceUserName, targetUserName, targetPassword, tableName, targetDsType, sourcePassword, \
        hostName, sourcePort, sourceFilePath, paramName, query, outputResultPath, targetTableName, \
        targetDriver, includeFeatures, updater = (' ') * 37

    zero = lambda n: [0 for _ in range(n)]
    nEpochs, batch_size, seed, iterations, learningRate, weightInit, num_leaves, min_data, \
        numInput, numOutputs, numHidden, numLayers, numHiddenLayers, output_dim, n_estimators, colsample_bylevel, \
        min_child_weight, random_state, reg_lambda, reg_alpha, scale_pos_weight, max_delta_step, width, height, poss_outcome, \
        interval, frameGroup, possOutcome, numIter, errorScore, verbose, numJobs, numFolds, numSplits, batchSize, \
        max_features, min_df, colsample_bynode, num_classes, max_leaves, n_jobs, verbosity, subsample_freq, subsample_for_bin, min_child_samples, gpu_id, \
        min_samples_split, min_samples_leaf, intercept_scaling, max_iter, n_clusters, n_init = zero(52)

    none = lambda n: [None for _ in range(n)]
    otherParams, paramList, sourceDsDetails, targetDsDetails, rowIdentifier, sourceAttrDetails, featureAttrDetails, sourceQuery, \
        inputSourceFileName, init, activation, optimizer, loss, average, encodingDetails, imputationDetails, modelSchema, sourceSchema, \
        inputColList, trainSetDetails, saveTrainingSet, trainSetDsType, trainSetTableName, trainSetSavePath, trainSetHostName, trainSetDbName, \
        trainSetPort, trainSetUserName, trainSetPassword, trainSetDriver, trainSetUrl, testSetDetails, testSetDsType, testSetTableName, \
        testSetSavePath, testSetHostName, testSetDbName, testSetPort, testSetUserName, testSetPassword, testSetDriver, testSetUrl, \
        boosting_type, metric, samplingTech, samplingType, objective, calcFeatureWeight, trainOverwrite, testOverwrite, resultOverwrite, \
        inputColLis, booster, mode, algoType, algoName, trainClass, resourceParams, filters, kernel_size, padding, pool_size, dropout, kernelInitializer, strides, poolSize, \
        kernelSize, metrics, optimiser, loss, kernelInitializer, seed, base_score, colsample_bylevel, colsample_bytree, gamma, max_depth, min_child_weight, n_estimators, reg_alpha, reg_lambda, lin_alpha, scale_pos_weight, cvType, max_depth, max_delta_step, learning_rate, execVersion, hyperParam, restartMode, checkPoint, checkPointDir, scoring, subsample, saveTrainVersion, scalingDetails, algoType, saveVersion, \
        saveTestVersion, grow_policy, missing, nthread, tree_method, silent, importance_type, interaction_constraints, monotone_constraints, validate_parameters, num_parallel_tree, class_weight, criterion, solver, splitter, presort, penalty, multi_class, max_leaf_nodes, n_iter_no_change, min_impurity_split, l1_ratio, max_samples, bootstrap, oob_score, warm_start, fit_intercept, normalize, dual, in_it, precompute_distances, copy_x, algorithm, edges_path, nodes_path = none(
        133)

    flo = lambda n: [0.0 for _ in range(n)]
    trainPercent, testPercent, sub_feature, base_score, colsample_bytree, subsample, max_df, min_split_gain, min_weight_fraction_leaf, min_impurity_decrease, C, tol, ccp_alpha, alpha, validation_fraction \
        = flo(15)
    output_result = dict()
    probThreshold = 0.5
    logLevel = 0
    checkpoint_dir = "Y"
    # driver_memory = '1g'

    os.environ["PYSPARK_PYTHON"] = sys.argv[1]
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    SUBMIT_ARGS = "--packages mysql:mysql-connector-java:8.0.24  pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    pyspark_submit_args = ' --driver-memory ' + '1g' + ' pyspark-shell'
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

    i = 0
    for eachArg in sys.argv:
        print(eachArg)
        if eachArg == "inputConfigFilePath":
            inputConfigFilePath = sys.argv[i + 1]
        i = i + 1

    print("Input Configuration File Path: ", inputConfigFilePath)

    with open(inputConfigFilePath, 'r') as json_file:
        input_config = json.load(json_file)

        for value in input_config:
            if value == "otherParams":
                otherParams = input_config[value]

        if otherParams != None:
            for value in otherParams:
                if value == "datasource": 
                    datasource = otherParams[value]  

                    if "sessionParam" in datasource:
                        session_param = datasource["sessionParam"]

                        # Extract nested keys
                        sourceCatalog = session_param.get("sourceCatalog")
                        sourceEndpoint = session_param.get("sourceEndpoint")
                        sourceAccessKey = session_param.get("sourceAccessKey")
                        sourceSecretKey = session_param.get("sourceSecretKey")
                        sourceRegion = session_param.get("sourceRegion")
                        
                        


                if value == "input_file_path":
                    input_file_path = otherParams[value]

                if value == "move_file_path":
                    move_file_path = otherParams[value]
                    
print(sourceCatalog)
print(sourceEndpoint)
print(sourceAccessKey)
print(sourceSecretKey)
print(sourceRegion)