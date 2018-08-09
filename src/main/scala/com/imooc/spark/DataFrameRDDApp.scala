package com.imooc.spark

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * DataFrame和RDD的互操作
 */
object DataFrameRDDApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    inferReflection(spark)

//    program(spark)

    spark.stop()
  }

  def program(spark: SparkSession): Unit = {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("E:\\temp\\student.txt")
    val infoRDD = rdd.map(_.split("\\|")).map(line => Row(line(0), line(1), line(2).toInt))

    val structType = StructType(Array(StructField("name", StringType, true),
      StructField("project", StringType, true),
      StructField("score", IntegerType, true)))

    val infoDF = spark.createDataFrame(infoRDD,structType)
//    infoDF.printSchema()
    infoDF.show()

//    //通过df的api进行操作
//    infoDF.filter(infoDF.col("age") > 30).show
//
//    //通过sql的方式进行操作
//    infoDF.createOrReplaceTempView("infos")
//    spark.sql("select * from infos where age > 30").show()
  }

  def inferReflection(spark: SparkSession) {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("E:\\temp\\student.txt")

    //注意：需要导入隐式转换
    import spark.implicits._
    val infoDF = rdd.map(_.split("\\|")).map(line => Info(line(0), line(1), line(2).toInt)).toDF()

    infoDF.show(5)

//    infoDF.filter(infoDF.col("age") > 30).show
//
//    infoDF.createOrReplaceTempView("infos")
//    spark.sql("select * from infos where age > 30").show()
  }

  case class Info(name: String,project: String, score: Int)

}
