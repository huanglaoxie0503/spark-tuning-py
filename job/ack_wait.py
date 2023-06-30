#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def use_off_heap_memory_ack_wait(spark_session):
    result = spark_session.sql("select * from spark_tuning.course_pay")
    result.cache()

    def print_orderid(iterator):
        for item in iterator:
            print(item.orderid)

    result.foreachPartition(print_orderid)


def ack_wait_tuning():
    # 连接超时时间，默认等于spark.network.timeout的值，默认120s
    spark_conf = SparkConf().setAppName("AckWaitTuning") \
        .set("spark.core.connection.ack.wait.timeout", "2s") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    use_off_heap_memory_ack_wait(spark_session)


if __name__ == '__main__':
    ack_wait_tuning()
