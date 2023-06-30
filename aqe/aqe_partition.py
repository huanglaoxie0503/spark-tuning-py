#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def use_join_aqe(spark_session):
    # 查询出三张表 并进行join 插入到最终表中
    sale_course = spark_session.sql("select *from spark_tuning.sale_courses")
    course_pay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")
    course_shopping_cart = spark_session.sql("select *from spark_tuning.course_shopping_cart") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    course_shopping_cart.join(course_pay, ["orderid"], "left") \
        .join(sale_course, ["courseid"], "right") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney", "cart_createtime", "pay_discount", "paymoney", "pay_createtime",
                "spark_tuning.sale_courses.dt",
                "spark_tuning.sale_courses.dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail_1")


def aqe_partition_tuning():
    """
    spark.sql.autoBroadcastJoinThreshold  为了演示效果，禁用广播join
    spark.sql.adaptive.enabled  开启AQE
    spark.sql.adaptive.coalescePartitions.enabled  合并分区的开关
    spark.sql.adaptive.coalescePartitions.initialPartitionNum   初始的并行度
    spark.sql.adaptive.coalescePartitions.minPartitionNum   合并后的最小分区数
    spark.sql.adaptive.advisoryPartitionSizeInBytes 合并后的分区，期望有多大
    :return:
    """
    spark_conf = SparkConf().setAppName("AQEPartitionTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1000") \
        .set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10") \
        .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "20mb") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    use_join_aqe(spark_session)


if __name__ == '__main__':
    aqe_partition_tuning()