package com.movies.movielens

import com.movies.spark.SparkSessionTestWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class MovieRatingsTest extends FlatSpec with BeforeAndAfter with SparkSessionTestWrapper {

  var movieRatings:MovieRatings = _

  before {
    val movies = getClass.getResource("/movies.dat").getPath
    val ratings = getClass.getResource("/ratings.dat").getPath
    val output = getClass.getResource("/").getPath
    val args:Array[String] = Array(movies,ratings,output)
    movieRatings = new MovieRatings(args,spark)
  }

  "movieRatings" should "meet prerequisites" in {
    assert(movieRatings.moviesFile===movieRatings.args(0))
    assert(movieRatings.ratingsFile===movieRatings.args(1))
    assert(movieRatings.outputDir===movieRatings.args(2))
  }

  "movieRatings" should "get the expected schemas" in {
    val moviesSchema = movieRatings.movieSchema
    val ratingsSchema = movieRatings.ratingsSchema
    val expectedMovieSchema = StructType(Seq(
      StructField("MovieID",IntegerType,nullable = false),
      StructField("Title",StringType,nullable = true),
      StructField("Genres",StringType,nullable = true)))
    val expectedRatingSchema = StructType(Seq(
      StructField("UserID",IntegerType,nullable = false),
      StructField("MovieID",IntegerType,nullable = false),
      StructField("Rating",IntegerType,nullable = false),
      StructField("Timestamp",StringType,nullable = true)))

    assert(moviesSchema===expectedMovieSchema)
    assert(ratingsSchema===expectedRatingSchema)
  }

  "movieRatings" should "match the join schema" in {
    val movieCols= movieRatings.movieCols.mkString(",")
    val joinCols = movieRatings.joinCols.mkString(",")
    val expectedMovieCols = "MovieID,Title,Genres"
    val expectedJoinCols = "MovieID,Title,Genres,Rating"
    assert(movieCols===expectedMovieCols)
    assert(joinCols===expectedJoinCols)
  }

}
