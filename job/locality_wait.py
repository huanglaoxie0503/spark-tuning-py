#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_cluster_session


def process_partition(p):
    for item in p:
        print(item.order_id)


def locality_wait_tuning():
    spark_conf = SparkConf().setAppName("LocalityWaitTuning") \
        .set("spark.locality.wait", "6s") \
        .set("spark.locality.wait.process", "60s") \
        .set("spark.locality.wait.node", "30s") \
        .set("spark.locality.wait.rack", "20s") \
        .setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    # 读取JSON文件并创建Dataset
    ds = spark_session.read.json("hdfs://node01:8020/spark_data/coursepay.log")
    ds.cache()

    ds.foreachPartition(process_partition)


if __name__ == '__main__':
    locality_wait_tuning()
