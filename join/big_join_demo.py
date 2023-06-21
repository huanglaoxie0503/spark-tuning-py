#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def use_join(spark_session):
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
                "sellmoney","cart_createtime", "pay_discount", "paymoney", "pay_createtime", "spark_tuning.sale_courses.dt",
                "spark_tuning.sale_courses.dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail_1")


def big_table_join():
    spark_conf = SparkConf().setAppName("BroadcastJoinTuning").set("spark.sql.shuffle.partitions", "36").setMaster(
        "local[*]")
    spark_session = get_spark_session(spark_conf=spark_conf)

    use_join(spark_session)

    # spark_session.sql("select count(*) from spark_tuning.sale_course_detail_1;").show()


if __name__ == '__main__':
    big_table_join()
