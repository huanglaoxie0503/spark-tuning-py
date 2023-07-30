#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf
from utils.common import get_spark_cluster_session


def get_explain():
    """
    获取 SQL 执行计划
        * ``simple``: Print only a physical plan.
        * ``extended``: Print both logical and physical plans.
        * ``codegen``: Print a physical plan and generated codes if they are available.
        * ``cost``: Print a logical plan and statistics if they are available.
        * ``formatted``: Split explain output into two sections: a physical plan outline \
            and node details.
    :return:
    """
    sparkConf = SparkConf().setAppName("ShowExplain").setMaster("local[*]")

    sparkSession = get_spark_cluster_session(spark_conf=sparkConf)

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

    print("====================explain()-只展示物理执行计划========================")
    sparkSession.sql(sqlQuery=sql_str).explain()

    print("=======================explain(mode = \"simple\")-只展示物理执行计划=============")
    sparkSession.sql(sqlQuery=sql_str).explain(mode="simple")
    #
    print("=================explain(mode = \"extended\")-展示物理执行计划和逻辑执行计划===========")
    sparkSession.sql(sqlQuery=sql_str).explain(mode="extended")

    print("==============explain(mode = \"formatted\")-以分隔的方式输出，它会输出更易读的物理执行计划，并展示每个节点的详细信息。========")
    sparkSession.sql(sqlQuery=sql_str).explain(mode="formatted")

    print("==============explain(mode = \"codegen\")-展示要 Codegen 生成的可执行 Java 代码划========")
    sparkSession.sql(sqlQuery=sql_str).explain(mode='codegen')
    print("==============explain(mode = \"cost\")-展示优化后的逻辑执行计划以及相关的统计========")
    sparkSession.sql(sqlQuery=sql_str).explain(mode='cost')


if __name__ == '__main__':
    get_explain()
