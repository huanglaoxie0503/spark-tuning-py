#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(spark_conf):
    spark_session = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    return spark_session


def get_explain():
    sparkConf = SparkConf().setAppName("ShowExplain").setMaster("local[*]")

    sparkSession = get_spark_session(spark_conf=sparkConf)

    sql_str = """
    select
      sc.courseid,
      sc.coursename,
      sum(sellmoney) as totalsell
    from sale_courses sc join course_shopping_cart csc
      on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
    group by sc.courseid,sc.coursename;
    """.strip()

    sparkSession.sql('use spark_tuning;')

    # sparkSession.sql(sqlQuery=sql_str).show()

    # print(
    #     "=====================================explain()-只展示物理执行计划============================================")
    # sparkSession.sql(sqlQuery=sql_str).explain()
    #
    # print(
    #     "===============================explain(mode = \"simple\")-只展示物理执行计划=================================")
    # sparkSession.sql(sqlQuery=sql_str).explain(mode="simple")
    #
    print(
        "============================explain(mode = \"extended\")-展示逻辑和物理执行计划==============================")
    sparkSession.sql(sqlQuery=sql_str).explain(mode="extended")

    # print(
    #     "============================explain(mode = \"formatted\")-展示格式化的物理执行计划=============================")
    # sparkSession.sql(sqlQuery=sql_str).explain(mode="formatted")


if __name__ == '__main__':
    get_explain()
