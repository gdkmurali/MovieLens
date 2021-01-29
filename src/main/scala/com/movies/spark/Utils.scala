package com.movies.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def readFile(file:String,schema:StructType,spark:SparkSession): DataFrame ={
    spark.read
      .format("csv")
      .options(Map("inferSchema"->"true","delimiter"->"::"))
      .schema(schema)
      .load(file)
  }

  def writeFile(df:DataFrame,dir:String) ={
    df.coalesce(1).write.parquet(dir)
  }

}
