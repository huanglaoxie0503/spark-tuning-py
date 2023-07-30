#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, StorageLevel
from utils.common import get_spark_cluster_session


def use_off_heap_memory(spark_session):
    result = spark_session.sql("select * from spark_tuning.course_pay")
    # TODO 指定持久化到堆外内存
    result.persist(StorageLevel.OFF_HEAP)

    def print_orderid(iterator):
        for item in iterator:
            print(item.order_id)

    result.foreachPartition(print_orderid)


def off_heap_cache_tuning():
    spark_conf = SparkConf().setAppName("OFFHeapCache").setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    use_off_heap_memory(spark_session)


if __name__ == '__main__':
    off_heap_cache_tuning()
