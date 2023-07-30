#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from pyspark.sql.functions import broadcast

from utils.common import get_spark_cluster_session, running_stop


def auto_broadcast_join():
    """
    通过参数指定自动广播：广播 join 默认值为 10MB，由 spark.sql.autoBroadcastJoinThreshold 参数控制。
    :return:
    """
    spark_conf = SparkConf().setAppName("BroadcastJoinTuning").set("spark.sql.autoBroadcastJoinThreshold",
                                                                   "10m").setMaster("local[*]")
    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    sql_str = """
        select
          sc.courseid,
          csc.courseid
        from sale_courses sc join course_shopping_cart csc
        on sc.courseid=csc.courseid
      """

    spark_session.sql("use spark_tuning;")
    spark_session.sql(sql_str).show()

    running_stop(50)


def force_broadcast_join():
    """
    强行广播
    :return:
    """
    # 关闭自动广播
    spark_conf = SparkConf().setAppName("BroadcastJoinTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .setMaster("local[*]")
    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    # TODO SQL Hint方式
    sql_str1 = """
        select /*+  BROADCASTJOIN(sc) */
          sc.courseid,
          csc.courseid
        from sale_courses sc join course_shopping_cart csc
        on sc.courseid=csc.courseid
      """.strip()

    sql_str2 = """
        select /*+  BROADCAST(sc) */
          sc.courseid,
          csc.courseid
        from sale_courses sc join course_shopping_cart csc
        on sc.courseid=csc.courseid
      """.strip()

    sql_str3 = """
        select /*+  MAPJOIN(sc) */
          sc.courseid,
          csc.courseid
        from sale_courses sc join course_shopping_cart csc
        on sc.courseid=csc.courseid
      """.strip()

    spark_session.sql("use spark_tuning;")

    print("=======================BROADCASTJOIN Hint=============================")
    spark_session.sql(sql_str1).explain()
    print("=======================BROADCAST Hint=============================")
    spark_session.sql(sql_str2).explain()
    print("=======================MAPJOIN Hint=============================")
    spark_session.sql(sql_str3).explain()

    # TODO API的方式
    sc = spark_session.sql("select * from sale_courses").toDF()
    csc = spark_session.sql("select * from course_shopping_cart").toDF()
    print("=======================DF API=============================")

    broadcast(sc).join(csc, list("courseid")).select("courseid").explain()


if __name__ == '__main__':
    # TODO 通过参数指定自动广播
    # auto_broadcast_join()

    # TODO 强行广播
    force_broadcast_join()
