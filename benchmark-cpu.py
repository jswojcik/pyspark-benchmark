#
#     This file is part of PySpark Benchmark.
# 
#     PySpark Benchmark is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
# 
#     PySpark Benchmark is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
# 
#     You should have received a copy of the GNU General Public License
#     along with PySpark Benchmark.  If not, see <https://www.gnu.org/licenses/>.
#
#     Originally written by Michael Kamprath <michael@kamprath.net>
#

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

import argparse
import random
import sys
from timeit import default_timer as timer


def parseArguments():
    arguments = argparse.ArgumentParser(
            description='Runs a CPU benchmarking of PySpark. Assumes you have used '
                        'generate-data.py to createa test data set.'
        )
    arguments.add_argument(
            'inputfile',
            type=str,
            metavar='file_url',
            help='The dataset to use. Input file URL.'
        )
    arguments.add_argument(
            '-s', '--pi-samples',
            metavar='num',
            type=int,
            default=5000000000,
            dest='piSamples',
            help='The number of samples used to calculate Pi'
        )
    arguments.add_argument(
            '-p', '--pi-parallelism',
            metavar='num',
            type=int,
            default=1000,
            dest='piParallelism',
            help='The number of tasks used to calculate Pi. Should be smaller than --pi-samples.'
        )
    arguments.add_argument(
            '-n', '--job-name',
            metavar='name',
            type=str,
            default='shuffle-benchmark',
            dest='appName',
            help='The name given this PySpark job'
        )
    return arguments.parse_args()

def benchmarkSHA256(df, jobLogger):
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting benchmark test calculatng SHA-512 hashes')
    start_time = timer()
    hashed_df = (
        df
        .withColumn('hashed_value', F.sha2(F.col('value'), 512))
    )

    # now trigger the computations by fetching a count at the RDD level
    count_value = hashed_df.rdd.count()
    end_time = timer()
    return (end_time-start_time), count_value

def benchmarkCalculatePi(spark, samples, parallelism, jobLogger):
    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting benchmark test calculatng Pi with {0:,} samples'.format(samples))
    start_time = timer()
    count = spark.sparkContext.parallelize(range(0, samples), parallelism).filter(inside).count()
    pi_val = 4.0*count/samples
    end_time = timer()
    return (end_time-start_time), pi_val
    

def main():
    args = parseArguments()

    spark = SparkSession.builder.appName(args.appName).getOrCreate()

    Logger= spark._jvm.org.apache.log4j.Logger
    joblogger = Logger.getLogger(__name__)
    joblogger.info('**********************************************************************')
    joblogger.info('')
    joblogger.info(
        'Benchmarking PySpark\'s CPU throughput using input data at {0}'.format(
            args.inputfile))
    joblogger.info('')
    joblogger.info('**********************************************************************')
    
    callSite_short_orig = spark.sparkContext.getLocalProperty('callSite.short')
    callSite_long_orig = spark.sparkContext.getLocalProperty('callSite.long')
   
    df = spark.read.csv(args.inputfile, header=True)

    spark.sparkContext.setLocalProperty('callSite.short', 'SHA-256-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark CPU calculating SHA-256 on a dataframe')
    sha256_time, sha256_hashes = benchmarkSHA256(df, joblogger)

    spark.sparkContext.setLocalProperty('callSite.short', 'calculate-pi-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark CPU calculating Pi')
    calcPi_time, pi_val = benchmarkCalculatePi(
        spark, args.piSamples, args.piParallelism, joblogger)

    #restore properties
    spark.sparkContext.setLocalProperty('callSite.short', callSite_short_orig)
    spark.sparkContext.setLocalProperty('callSite.long', callSite_long_orig)

    joblogger.info('****************************************************************************')
    joblogger.info('    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS')
    joblogger.info('    Test Run = {0}'.format(args.appName))
    joblogger.info('')
    joblogger.info('SHA-512 benchmark time  = {0} seconds for {1:,} hashes'.format(
                        sha256_time, sha256_hashes))
    joblogger.info('Calculate Pi benchmark  = {0} seconds with pi = {1}, samples = {2:,}'.format(
                        calcPi_time, pi_val, args.piSamples))
    joblogger.info('')
    joblogger.info('****************************************************************************')


if __name__ == '__main__':
    main()
