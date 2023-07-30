#!/usr/bin/python
# -*- coding:UTF-8 -*-
from random import randint

from pyspark import SparkConf

from utils.common import get_spark_cluster_session


# Define the UDFs
def random_prefix_udf(value, num):
    return f"{randint(0, num)}_{value}"


def remove_random_prefix_udf(value):
    return value.split("_", 1)[1]


def skew_aggregation_tuning():
    spark_conf = SparkConf().setAppName("SkewAggregationTuning") \
        .set("spark.sql.shuffle.partitions", "36") \
        .setMaster("local[*]")

    spark_session = get_spark_cluster_session(spark_conf=spark_conf)

    spark_session.udf.register("random_prefix", random_prefix_udf)
    spark_session.udf.register("remove_random_prefix", remove_random_prefix_udf)

    # sparkSession.udf.register("random_prefix", ( value: Int, num: Int ) => randomPrefixUDF(value, num))
    # sparkSession.udf.register("remove_random_prefix", ( value: String ) => removeRandomPrefixUDF(value))

    sql1 = """
        select
          courseid,
          sum(course_sell) totalSell
        from
          (
            select
              remove_random_prefix(random_courseid) courseid,
              course_sell
            from
              (
                select
                  random_courseid,
                  sum(sellmoney) course_sell
                from
                  (
                    select
                      random_prefix(courseid, 6) random_courseid,
                      sellmoney
                    from
                      spark_tuning.course_shopping_cart
                  ) t1
                group by random_courseid
              ) t2
          ) t3
        group by courseid
      """.strip()

    sql2 = """
        select
          courseid,
          sum(sellmoney)
        from spark_tuning.course_shopping_cart
        group by courseid
      """.strip()

    spark_session.sql(sql1).show(10000)


if __name__ == '__main__':
    skew_aggregation_tuning()