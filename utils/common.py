#!/usr/bin/python
# -*- coding:UTF-8 -*-
import time
from pyspark.sql import SparkSession
from pyspark import SparkConf


def running_stop(num=60):
    n = 0
    while True:
        print(n)
        time.sleep(num)
        n += 1


def get_spark_session(spark_conf):
    spark_session = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    # spark_session.sparkContext
    return spark_session


def init_hive_table(spark_session):
    spark_session.read.json('/sparkdata/salecourse.log'). \
        write.partitionBy('dt', 'dn').format('parquet'). \
        mode('overwrite'). \
        saveAsTable('spark_tuning.sale_courses')

    # spark_session.read.json('/sparkdata/coursepay.log'). \
    #     write.partitionBy('dt', 'dn').format('parquet'). \
    #     mode('overwrite'). \
    #     saveAsTable('spark_tuning.course_pay')
    #
    # spark_session.read.json('/sparkdata/courseshoppingcart.log'). \
    #     write.partitionBy('dt', 'dn').format('parquet'). \
    #     mode('overwrite'). \
    #     saveAsTable('spark_tuning.course_shopping_cart')


def init_bucket_table(spark_session):
    spark_session.read.json('/sparkdata/coursepay.log') \
        .write \
        .partitionBy('dt', 'dn') \
        .format('parquet') \
        .bucketBy(5, 'orderid') \
        .sortBy('orderid') \
        .mode('overwrite') \
        .saveAsTable('spark_tuning.course_pay_cluster')

    spark_session.read.json('/sparkdata/courseshoppingcart.log') \
        .write \
        .partitionBy('dt', 'dn') \
        .format('parquet') \
        .bucketBy(5, 'orderid') \
        .sortBy('orderid') \
        .mode('overwrite') \
        .saveAsTable('spark_tuning.course_shopping_cart_cluster')


if __name__ == '__main__':
    sparkConf = SparkConf().setAppName("ExplainDemo").setMaster('local[*]')
    sparkConf.set('fs.defaultFS', 'hdfs://Oscar-MacPro:8020')
    sparkSession = get_spark_session(sparkConf)

    init_hive_table(spark_session=sparkSession)

    # init_bucket_table(spark_session=sparkSession)