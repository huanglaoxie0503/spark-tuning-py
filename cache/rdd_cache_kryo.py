#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, StorageLevel

from utils.common import get_spark_cluster_session


class CoursePay(object):

    def __init__(self, order_id, dis_count, pay_money, create_time, dt, dn):
        self.order_id = order_id
        self.dis_count = dis_count
        self.pay_money = pay_money
        self.create_time = create_time
        self.dt = dt
        self.dn = dn


def print_order_id(iterator):
    for item in iterator:
        print(item.order_id)


def rdd_cache_kryo_demo(spark_session):
    result = spark_session.sql('select * from spark_tuning.course_pay')
    result.persist(StorageLevel.MEMORY_ONLY)
    result.foreachPartition(print_order_id)
    while True:
        pass


if __name__ == '__main__':
    # 集群模式
    conf = SparkConf()
    conf.setAppName('RddCacheKryoDemo')
    conf.setMaster('local[*]')
    conf.set('fs.defaultFS', 'hdfs://node01:8020')
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set('spark.kryo.classesToRegister', "CoursePay")
    sparkSession = get_spark_cluster_session(spark_conf=conf)
