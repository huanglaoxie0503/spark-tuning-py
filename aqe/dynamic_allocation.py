#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from aqe.aqe_partition import use_join_aqe
from utils.common import get_spark_cluster_session


def dynamic_allocation_tuning():
    """
    spark.dynamicAllocation.enabled 动态申请资源
    spark.dynamicAllocation.shuffleTracking.enabled shuffle动态跟踪
    :return:
    """
    spark_conf = SparkConf().setAppName("DynamicAllocationTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1000") \
        .set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10") \
        .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "20mb") \
        .set("spark.dynamicAllocation.enabled", "true") \
        .set("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
        .setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    use_join_aqe(spark_session)


if __name__ == '__main__':
    dynamic_allocation_tuning()