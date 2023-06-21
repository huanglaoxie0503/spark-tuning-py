#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session, running_stop


def map_small_file_tuning():
    """
    小文件优化：
        spark.files.openCostInBytes 默认4M，设置成和小文件大小相当
        spark.sql.files.maxPartitionBytes   默认128M 最大分区
    :return:
    """
    spark_conf = SparkConf().setAppName("MapSmallFileTuning") \
        .set("spark.files.openCostInBytes", "7194304") \
        .set("spark.sql.files.maxPartitionBytes", "128MB") \
        .setMaster("local[1]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    spark_session.sql("select * from spark_tuning.course_shopping_cart")\
        .write.mode('overwrite').saveAsTable("spark_tuning.small_file_test")


if __name__ == '__main__':
    map_small_file_tuning()
    running_stop()