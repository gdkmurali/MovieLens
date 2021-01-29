package com.movies.movielens

import com.movies.entities.{movies, ratings}
import com.movies.spark.{SparkSessionWrapper, Utils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object MovieRatings extends App with SparkSessionWrapper {
  val mr = new MovieRatings(args,spark)
  mr.transform()
}

class MovieRatings(val args:Array[String],val spark:SparkSession) {
  val moviesFile = args(0)
  val ratingsFile = args(1)
  val outputDir = args(2)

  val movieSchema: StructType = Encoders.product[movies].schema
  val ratingsSchema: StructType = Encoders.product[ratings].schema

  val movieCols = movieSchema.fieldNames.map(c=>col(c))
  val joinCols: Array[Column] = movieCols :+ col("Rating")

  def transform()={
    val moviesDF = Utils.readFile(moviesFile,movieSchema,spark)
    val ratingsDF = Utils.readFile(ratingsFile,ratingsSchema,spark)
    val joinedDF = moviesDF.join(ratingsDF,"MovieID").select(joinCols:_*)
    val finalDF = joinedDF.groupBy(movieCols:_*)
                  .agg(
                    min("Rating").as("MinRating"),
                    max("Rating").as("MaxRating"),
                    avg("Rating").as("AvgRating")
                  )
    Utils.writeFile(moviesDF,outputDir+"/movies")
    Utils.writeFile(ratingsDF,outputDir+"/ratings")
    Utils.writeFile(finalDF,outputDir+"/movieRatings")
  }
}
