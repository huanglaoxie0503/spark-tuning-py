#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def reduce_shuffle_tuning():
    """
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") # 为了演示效果，先禁用了广播join
      .set("spark.sql.shuffle.partitions", "36")
      .set("spark.reducer.maxSizeInFlight", "96m") # reduce缓冲区，默认48m
      .set("spark.shuffle.io.maxRetries", "6")  #  重试次数，默认3次
      .set("spark.shuffle.io.retryWait", "60s")  #  重试的间隔，默认5s

    :return: 
    """
    spark_conf = SparkConf().setAppName("SkewMapJoinTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "36") \
        .set("spark.reducer.maxSizeInFlight", "96m") \
        .set("spark.shuffle.io.maxRetries", "6") \
        .set("spark.shuffle.io.retryWait", "60s") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    # 查询出三张表 并进行join 插入到最终表中
    saleCourse = spark_session.sql("select * from spark_tuning.sale_courses")
    coursePay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")

    courseShoppingCart = spark_session.sql("select * from spark_tuning.course_shopping_cart") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    saleCourse.join(courseShoppingCart, ["courseid", "dt", "dn"], "right") \
        .join(coursePay, ["orderid", "dt", "dn"], "left") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney","cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail")


if __name__ == '__main__':
    reduce_shuffle_tuning()