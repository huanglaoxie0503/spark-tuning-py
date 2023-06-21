#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def bypass_tuning():
    """
        .set("spark.shuffle.sort.bypassMergeThreshold", "30") //bypass阈值，默认200,改成30对比效果
    :return:
    """
    spark_conf = SparkConf().setAppName("BypassTuning") \
        .set("spark.sql.shuffle.partitions", "36") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    # 查询出三张表 并进行join 插入到最终表中
    saleCourse = spark_session.sql("select * from spark_tuning.sale_course")
    coursePay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")

    courseShoppingCart = spark_session.sql("select * from spark_tuning.course_shopping_cart") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime") \

    saleCourse.join(courseShoppingCart, ["courseid", "dt", "dn"], "right") \
        .join(coursePay, ["orderid", "dt", "dn"], "left") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney","cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail")


if __name__ == '__main__':
    bypass_tuning()