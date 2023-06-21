#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf

from utils.common import get_spark_session


def cbo_statics_collect():
    spark_conf = SparkConf().setAppName("CBO").setMaster("local[*]")
    spark_session = get_spark_session(spark_conf=spark_conf)

    analyze_table_and_column(spark_session, "sparktuning.sale_course", "courseid,dt,dn")
    analyze_table_and_column(spark_session, "sparktuning.course_shopping_cart", "courseid,orderid,dt,dn")
    analyze_table_and_column(spark_session, "sparktuning.course_pay", "orderid,dt,dn")


def analyze_table_and_column(spark_session, table_name, column_list=None):
    # TODO 查看 表级别 信息
    print(
        "=========================================查看" + table_name + "表级别 信息========================================")
    spark_session.sql("DESC FORMATTED " + table_name).show(100)
    # TODO 统计 表级别 信息
    print(
        "=========================================统计 " + table_name + "表级别 信息========================================")
    spark_session.sql("ANALYZE TABLE " + table_name + " COMPUTE STATISTICS").show()
    # TODO 再查看 表级别 信息
    print(
        "======================================查看统计后 " + table_name + "表级别 信息======================================")
    spark_session.sql("DESC FORMATTED " + table_name).show(100)

    # TODO 查看列级别信息
    print(
        "=========================================查看 " + table_name + "表的" + column_list + "列级别 信息========================================")
    columns = column_list.split(",")
    for column in columns:
        spark_session.sql("DESC FORMATTED " + table_name + " " + column).show()

    # TODO 统计 列级别 信息
    print("=========================================统计 " + table_name + "表的" + column_list + "列级别 信息========================================")
    spark_session.sql(
        """
         ANALYZE TABLE {0}
         COMPUTE STATISTICS
         FOR COLUMNS {1}
      """.format(table_name, column_list).strip()).show()

    # TODO 再查看 列级别 信息
    print("======================================查看统计后 " + table_name + "表的" + column_list + "列级别 信息======================================")
    for column in columns:
        spark_session.sql("DESC FORMATTED " + table_name + " " + column).show()


if __name__ == '__main__':
    cbo_statics_collect()
