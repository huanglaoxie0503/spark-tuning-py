#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_cluster_session, create_spark_session


def print_orderid(iterator):
    for item in iterator:
        print(item.orderid)


def rdd_cache_demo(spark_session):
    result = spark_session.sql('select * from spark_tuning.course_pay').rdd
    result.cache()
    result.foreachPartition(print_orderid)
    while True:
        pass


if __name__ == '__main__':
    # 集群模式
    conf = SparkConf()
    conf.setAppName('RddCache')
    conf.setMaster('local[*]')
    conf.set('fs.defaultFS', 'hdfs://node01:8020')
    sparkSession = get_spark_cluster_session(spark_conf=conf)

    rdd_cache_demo(spark_session=sparkSession)
