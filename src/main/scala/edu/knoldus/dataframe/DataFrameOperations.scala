package edu.knoldus.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
class DataFrameOperations {

  def readCsvFile(filePath: String, sparkContext: SparkSession): DataFrame = {
    sparkContext.read.option("header", "true").option("inferSchema","true").csv(filePath)
  }

  def getHomeMatchesCount(df: DataFrame, sparkContext: SparkSession): DataFrame = {
    df.createOrReplaceTempView("Football")
    sparkContext.sql("SELECT HomeTeam, count(FTR) FROM Football GROUP BY HomeTeam")
  }

  def getTopTenWinningTeams(df: DataFrame, sparkContext: SparkSession): DataFrame = {
    df.createOrReplaceTempView("Football")
    val totalHomeMatches = sparkContext.sql("SELECT HomeTeam AS Teams, count(FTR) AS FTR FROM Football GROUP BY HomeTeam")
    val totalAwayMatches = sparkContext.sql("SELECT AwayTeam AS Teams, count(FTR) AS FTR FROM Football GROUP BY AwayTeam")
    val totalMatches = totalHomeMatches.union(totalAwayMatches).groupBy("Teams").sum("FTR").withColumnRenamed("sum(FTR)", "Matches")
    val homeWinCount = sparkContext.sql("SELECT HomeTeam AS Teams, count(FTR) AS FTR FROM Football WHERE FTR = 'H' GROUP BY HomeTeam")
    val awayWinCount = sparkContext.sql("SELECT AwayTeam AS Teams, count(FTR) AS FTR FROM Football WHERE FTR = 'A' GROUP BY AwayTeam")
    val totalWins = homeWinCount.union(awayWinCount).groupBy("Teams").sum("FTR").withColumnRenamed("sum(FTR)", "Wins")
    val joinedDf = totalWins.join(totalMatches, Seq("Teams"), "inner")
    joinedDf.withColumn("Percentage", col("Wins") / col("Matches") * 100).select("Teams", "Percentage").orderBy(desc("Percentage")).limit(10)
  }

}
