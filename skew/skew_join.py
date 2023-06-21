#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from random import Random
from random import randint
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from utils.common import get_spark_session, running_stop


# 定义打散函数
def map_partitions(partitions):
    for item in partitions:
        courseid = item["courseid"]
        rand_int = randint(0, 35)
        yield Row(
            courseid=courseid,
            orderid=item["orderid"],
            coursename=item["coursename"],
            cart_discount=item["cart_discount"],
            sellmoney=item["sellmoney"],
            cart_createtime=item["cart_createtime"],
            dt=item["dt"],
            dn=item["dn"],
            rand_courseid=f"{rand_int}_{courseid}"
        )


# 定义扁平化函数
def flatten_row(row):
    items = []
    courseid = row.courseid
    coursename = row.coursename
    status = row.status
    pointlistid = row.pointlistid
    majorid = row.majorid
    chapterid = row.chapterid
    chaptername = row.chaptername
    edusubjectid = row.edusubjectid
    edusubjectname = row.edusubjectname
    teacherid = row.teacherid
    teachername = row.teachername
    coursemanager = row.coursemanager
    money = row.money
    dt = row.dt
    dn = row.dn
    for i in range(36):
        items.append((courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
                      edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, f"{i}_{courseid}"))
    return items


def scatter_big_and_expansion_small(spark_session):
    """
    打散大表  扩容小表 解决数据倾斜
    :param spark_session:
    :return:
    """
    sale_course = spark_session.sql("select *from spark_tuning.sale_courses")
    course_pay = spark_session.sql("select * from spark_tuning.course_pay") \
        .withColumnRenamed("discount", "pay_discount") \
        .withColumnRenamed("createtime", "pay_createtime")

    course_shopping_cart = spark_session.sql("select * from spark_tuning.course_shopping_cart") \
        .withColumnRenamed("discount", "cart_discount") \
        .withColumnRenamed("createtime", "cart_createtime")

    # TODO 1、拆分 倾斜的key result = rdd.filter(lambda x: x % 2 == 1)
    # 多条件过滤
    # result = data.filter((col("column1") > 10) & (col("column2") == "value"))
    # 未倾斜
    commonCourseShoppingCart = course_shopping_cart.filter((col("courseid") != 101) & (col("courseid") != 103))
    skew_course_shopping_cart = course_shopping_cart.filter((col("courseid") != 101) & (col("courseid") != 103))

    # 对 DataFrame 进行打散操作
    newCourseShoppingCart = skew_course_shopping_cart.rdd.mapPartitions(map_partitions).toDF()
    print("-------newCourseShoppingCart-----")
    # newCourseShoppingCart.show()

    # TODO 3、小表进行扩容扩大36倍
    # 对 DataFrame 进行扁平化操作
    newSaleCourse = sale_course.rdd.flatMap(flatten_row)
    # 转换为 DataFrame
    newSaleCourseSchema = StructType([
        StructField("courseid", LongType(), True),
        StructField("coursename", StringType(), True),
        StructField("status", StringType(), True),
        StructField("pointlistid", LongType(), True),
        StructField("majorid", LongType(), True),
        StructField("chapterid", LongType(), True),
        StructField("chaptername", StringType(), True),
        StructField("edusubjectid", LongType(), True),
        StructField("edusubjectname", StringType(), True),
        StructField("teacherid", LongType(), True),
        StructField("teachername", StringType(), True),
        StructField("coursemanager", StringType(), True),
        StructField("money", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("dn", StringType(), True),
        StructField("rand_courseid", StringType(), True)
    ])

    newSaleCourse = spark_session.createDataFrame(newSaleCourse, schema=newSaleCourseSchema)
    print("---------newSaleCourse---------")
    # newSaleCourse.show()

    spark_session.sql("DESCRIBE spark_tuning.course_pay").show()

    # TODO 4、倾斜的大key 与  扩容后的表 进行join
    df1 = newSaleCourse.join(newCourseShoppingCart.drop("courseid").drop("coursename"), ["rand_courseid", "dt", "dn"],
                             "right") .join(course_pay, ["orderid", "dt", "dn"], "left") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney", "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
    df1.show()
    # // TODO 5、没有倾斜大key的部分 与 原来的表 进行join
    df2 = sale_course.join(commonCourseShoppingCart.drop("coursename"), ["courseid", "dt", "dn"], "right") \
        .join(course_pay, ["orderid", "dt", "dn"], "left") \
        .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount",
                "sellmoney",
                "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")

    df2.show()

    # TODO 6、将 倾斜key join后的结果 与 普通key join后的结果，uinon 起来
    df1.union(df2).limit(10000).write.mode('overwrite').saveAsTable("spark_tuning.sale_course_detail")


def skew_join_tuning():
    spark_conf = SparkConf().setAppName("SkewJoinTuning") \
        .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .set("spark.sql.shuffle.partitions", "36") \
        .setMaster("local[*]")
    spark_session = get_spark_session(spark_conf=spark_conf)

    scatter_big_and_expansion_small(spark_session)

    running_stop()


if __name__ == '__main__':
    skew_join_tuning()
