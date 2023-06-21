#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from utils.common import get_spark_session


def sample_top_key(spark_session, table_name, key_column):
    df = spark_session.sql("select " + key_column + " from " + table_name)
    top_10_key = df.select(key_column).sample(withReplacement=False, fraction=0.1).rdd \
        .map(lambda k: (k, 1)).reduceByKey(lambda a, b: a + b) \
        .map(lambda k: (k[1], k[0])).sortByKey(ascending=False) \
        .take(10)
    return top_10_key


def sample_key():
    spark_conf = SparkConf().setAppName("SampleKeyDemo") \
        .set("spark.sql.shuffle.partitions", "36") \
        .setMaster("local[*]")
    spark_session = get_spark_session(spark_conf=spark_conf)

    print(
        "=============================================csc courseid sample=============================================")
    csc_top_key = sample_top_key(spark_session, "spark_tuning.course_shopping_cart", "courseid")
    print(csc_top_key)

    print(
        "=============================================sc courseid sample=============================================")
    sc_top_Key = sample_top_key(spark_session, "spark_tuning.sale_courses", "courseid")
    print(sc_top_Key)

    print("=============================================cp orderid sample=============================================")
    cp_top_key = sample_top_key(spark_session, "spark_tuning.course_pay", "orderid")
    print(cp_top_key)

    print(
        "=============================================csc orderid sample=============================================")
    csc_top_order_key = sample_top_key(spark_session, "spark_tuning.course_shopping_cart", "orderid")
    print(csc_top_order_key)


if __name__ == '__main__':
    sample_key()
