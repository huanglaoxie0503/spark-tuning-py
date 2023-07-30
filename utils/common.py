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


def get_spark_local_session(spark_conf):
    """
    连接本地开发环境
    :param spark_conf: sparkConf
    :return:
    """
    spark_session = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    return spark_session


def get_spark_cluster_session(spark_conf):
    """
    连接集群环境
    :param spark_conf: sparkConf
    :return:
    """
    warehouse_dir = "hdfs://node01:8020/user/hive/warehouse"
    metastore_uri = "thrift://node01:9083"

    spark_session = SparkSession.builder \
        .appName("InitData") \
        .master("local[*]") \
        .config(conf=spark_conf) \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("hive.metastore.uris", metastore_uri) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark_session


def init_hive_table(spark_session):
    spark_session.read.json('/spark_data/salecourse.log'). \
        write.partitionBy('dt', 'dn').format('parquet'). \
        mode('overwrite'). \
        saveAsTable('spark_tuning.sale_courses')

    spark_session.read.json('/spark_data/coursepay.log'). \
        write.partitionBy('dt', 'dn').format('parquet'). \
        mode('overwrite'). \
        saveAsTable('spark_tuning.course_pay')

    spark_session.read.json('/spark_data/courseshoppingcart.log'). \
        write.partitionBy('dt', 'dn').format('parquet'). \
        mode('overwrite'). \
        saveAsTable('spark_tuning.course_shopping_cart')


def init_bucket_table(spark_session):
    spark_session.read.json('/spark_data/coursepay.log') \
        .write \
        .partitionBy('dt', 'dn') \
        .format('parquet') \
        .bucketBy(5, 'orderid') \
        .sortBy('orderid') \
        .mode('overwrite') \
        .saveAsTable('spark_tuning.course_pay_cluster')

    spark_session.read.json('/spark_data/courseshoppingcart.log') \
        .write \
        .partitionBy('dt', 'dn') \
        .format('parquet') \
        .bucketBy(5, 'orderid') \
        .sortBy('orderid') \
        .mode('overwrite') \
        .saveAsTable('spark_tuning.course_shopping_cart_cluster')


if __name__ == '__main__':
    # 本地模式
    # sparkConf = SparkConf().setAppName("InitData").setMaster('local[*]')
    # sparkConf.set('fs.defaultFS', 'hdfs://Oscar-MacPro:8020')
    # sparkSession = get_spark_local_session(spark_conf=sparkConf)

    # 集群模式
    conf = SparkConf()
    conf.set('fs.defaultFS', 'hdfs://node01:8020')
    sparkSession = get_spark_cluster_session(spark_conf=conf)

    init_hive_table(spark_session=sparkSession)

    init_bucket_table(spark_session=sparkSession)
    """
    # yarn 集群提交命令
     spark-submit --master yarn --deploy-mode client --driver-memory 1g --num-executors 3 --executor-cores 3 --executor-memory 1g ./spark-tuning-py/utils/common.py 
    """
