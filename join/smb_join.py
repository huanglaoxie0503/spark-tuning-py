#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from pyspark.sql.functions import broadcast
from utils.common import get_spark_cluster_session


def use_smb_join(spark_session):
    sale_course = spark_session.sql("select *from spark_tuning.sale_courses")
    course_pay = spark_session.sql("select * from spark_tuning.course_pay_cluster") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")

    course_shopping_cart = spark_session.sql("select *from spark_tuning.course_shopping_cart_cluster") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    tmp_data = course_shopping_cart.join(course_pay, ["orderid"], "left")

    result = broadcast(sale_course).join(tmp_data, ["courseid"], "right")

    result.select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername",
                  "edusubjectid"
                  , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                  "sellmoney",
                  "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "spark_tuning.sale_courses.dt",
                  "spark_tuning.sale_courses.dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail_2")


def smb_join_tuning():
    spark_conf = SparkConf().setAppName("BroadcastJoinTuning") \
        .set("spark.sql.shuffle.partitions", "36").setMaster("local[*]")
    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    use_smb_join(spark_session)
    # spark_session.sql("DESCRIBE spark_tuning.sale_courses").show()
    # spark_session.sql("select count(*) from spark_tuning.sale_course_detail_2;").show()


if __name__ == '__main__':
    smb_join_tuning()
