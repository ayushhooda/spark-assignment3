package edu.knoldus.dataset

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DatasetOperations {

  def convertDataFrameToDataSet(df: DataFrame, spark: SparkSession): Dataset[DataSetTable] = {
    import spark.implicits._
    df.as[DataSetTable]
  }

  def getTotalMatchesForEachTeam(ds: Dataset[DataSetTable], spark: SparkSession): Dataset[(String, BigInt)] = {
    import spark.implicits._
    val homeTeam: DataFrame = ds.select("HomeTeam").withColumnRenamed("HomeTeam", "Teams")
    val awayTeam: DataFrame = ds.select("AwayTeam").withColumnRenamed("AwayTeam", "Teams")
    val totalMatches: DataFrame = homeTeam.union(awayTeam).groupBy("Teams").count()
    totalMatches.as[(String, BigInt)]
  }

//  def getTopTenTeamsWithHighestWin(ds: Dataset[DataSetTable]): Dataset[(String, Int)] = {
//
//  }
}

