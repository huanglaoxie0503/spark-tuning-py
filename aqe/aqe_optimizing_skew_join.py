#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def skew_join_strategies(spark_session):
    saleCourse = spark_session.sql("select *from spark_tuning.sale_courses")

    coursePay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")

    courseShoppingCart = spark_session.sql("select *from spark_tuning.course_shopping_cart") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    saleCourse.join(courseShoppingCart, ["courseid", "dt", "dn"], "right") \
        .join(coursePay, ["orderid", "dt", "dn"], "left") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney","cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn") \
        .write.mode('overwrite').insertInto("spark_tuning.sale_course_detail_2")


def aqe_optimizing_skew_join_tuning():
    """
    spark.sql.autoBroadcastJoinThreshold    为了演示效果，禁用广播join
    spark.sql.adaptive.coalescePartitions.enabled   为了演示效果，关闭自动缩小分区
    :return:
    """
    spark_conf = SparkConf().setAppName("DynamicAllocationTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.skewJoin.enable", "true") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2") \
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "20mb") \
        .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8mb") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    skew_join_strategies(spark_session)


if __name__ == '__main__':
    aqe_optimizing_skew_join_tuning()
