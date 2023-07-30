#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_cluster_session


def map_file_buffer_tuning():
    """
    增大 map 溢写时输出流的 buffer
        .set("spark.sql.shuffle.partitions", "36")
        .set("spark.shuffle.file.buffer", "64") # 对比 shuffle write 的stage 耗时
        .set("spark.shuffle.spill.batchSize", "20000") # 不可修改
        .set("spark.shuffle.spill.initialMemoryThreshold", "104857600") # 不可修改

    :return:
    """
    spark_conf = SparkConf().setAppName("MapFileBufferTuning") \
        .set("spark.sql.shuffle.partitions", "36") \
        .set("spark.shuffle.file.buffer", "64") \
        .setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

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
                "sellmoney",
                "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn") \
        .write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail")


if __name__ == '__main__':
    map_file_buffer_tuning()
