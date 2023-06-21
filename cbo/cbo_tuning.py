#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session

"""
spark.sql.cbo.enabled：CBO 总开关。true 表示打开，false 表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成。
spark.sql.cbo.joinReorder.enabled：使用 CBO 来自动调整连续的 inner join 的顺序 true：表示打开，false：表示关闭要使用该功能，需确保相关表和列的统计信息已经生成，且CBO 总开关打开。
spark.sql.cbo.joinReorder.dp.threshold: 使用 CBO 来自动调整连续 inner join 的表的个数阈值。如果超出该阈值，则不会调整 join 顺序。
"""


def tuning():
    # spark_conf = SparkConf().setAppName("CBO").setMaster("local[*]")
    spark_conf = SparkConf().setAppName("CBOTuning").set("spark.sql.cbo.enabled", "true").setMaster("local[*]")
    #
    # spark_conf = SparkConf().setAppName("CBO") \
    #     .set("spark.sql.cbo.enabled", "true") \
    #     .set("spark.sql.cbo.joinReorder.enabled", "true") \
    #     .set("spark.sql.cbo.joinReorder.dp.threshold", "") \
    #     .setMaster("local[*]")

    spark_session = get_spark_session(spark_conf=spark_conf)

    sql_str = """
      select
        csc.courseid,
        sum(cp.paymoney) as coursepay
      from course_shopping_cart csc,course_pay cp
      where csc.orderid=cp.orderid
      and cp.orderid ='odid-0'
      group by csc.courseid
    """

    spark_session.sql("use spark_tuning;")
    spark_session.sql(sql_str).show()

    n = 0
    while True:
        print(n)
        import time
        time.sleep(60)
        n += 1


if __name__ == '__main__':
    tuning()
