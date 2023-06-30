#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def switch_join_strategies(spark_session):
    course_pay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime") \
        .where("orderid between 'odid-9999000' and 'odid-9999999'")

    course_shopping_cart = spark_session.sql("select *from spark_tuning.course_shopping_cart") \
        .drop("coursename") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    tmp_data = course_pay.join(course_shopping_cart, ["orderid"], "right")

    tmp_data.show()


def aqe_dynamic_switch_join_tuning():
    """
    spark.sql.adaptive.localShuffleReader.enabled   在不需要进行shuffle重分区时，尝试使用本地shuffle读取器。将sort-meger join 转换为广播join
    :return:
    """
    spark_conf = SparkConf().setAppName("AqeDynamicSwitchJoin") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    switch_join_strategies(spark_session)


if __name__ == '__main__':
    aqe_dynamic_switch_join_tuning()