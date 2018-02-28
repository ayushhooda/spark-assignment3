package edu.knoldus.dataset

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DatasetOperations {

  def convertDataFrameToDataSet(df: DataFrame, spark: SparkSession): Dataset[DataSetTable] = {
    import spark.implicits._
    df.as[DataSetTable]
  }

  def getTotalMatchesForEachTeam(ds: Dataset[DataSetTable], spark: SparkSession): Dataset[(String, BigInt)] = {
    import spark.implicits._
    val homeTeam = ds.select("HomeTeam").withColumnRenamed("HomeTeam", "Teams")
    val awayTeam = ds.select("AwayTeam").withColumnRenamed("AwayTeam", "Teams")
    val totalMatches = homeTeam.union(awayTeam).groupBy("Teams").count()
    totalMatches.as[(String, BigInt)]
  }

  def getTopTenTeamsWithHighestWin(ds: Dataset[DataSetTable], spark: SparkSession): Dataset[(String, BigInt)] = {
    import spark.implicits._
    val homeTeamDF = ds.select("HomeTeam","FTR").where("FTR = 'H'" ).groupBy("HomeTeam").
      count().withColumnRenamed("count", "Wins").withColumnRenamed("HomeTeam", "Teams")
    val awayTeamDF = ds.select("AwayTeam","FTR").where("FTR = 'A'").groupBy("AwayTeam").
      count().withColumnRenamed("count", "Wins").withColumnRenamed("AwayTeam", "Teams")
    val teamsDF: DataFrame = homeTeamDF.union(awayTeamDF).groupBy("Teams").sum("Wins").orderBy(sortCol = "sum(Wins)").limit(10)
    teamsDF.as[(String, BigInt)]
  }
}

