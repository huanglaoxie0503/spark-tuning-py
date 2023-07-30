#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_cluster_session


def dynamic_partition_small_file_tuning():
    spark_conf = SparkConf().setAppName("DynamicPartitionSmallFileTuning") \
        .set("spark.sql.shuffle.partitions", "36") \
        .setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    # TODO 非倾斜分区写入
    spark_session.sql(
      """
        insert overwrite spark_tuning.dynamic_csc partition(dt,dn)
        select * from spark_tuning.course_shopping_cart
        where dt!='20190722' and dn!='webA'
        distribute by dt,dn
      """.strip()).show()

    # TODO 倾斜分区打散写入
    spark_session.sql(
      """
        insert overwrite spark_tuning.dynamic_csc partition(dt,dn)
        select * from spark_tuning.course_shopping_cart
        where dt='20190722' and dn='webA'
        distribute by cast(rand() * 5 as int)
      """.strip()).show()


if __name__ == '__main__':
    dynamic_partition_small_file_tuning()
