import edu.knoldus.dataframe.DataFrameOperations
import edu.knoldus.dataset.DatasetOperations
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  //scalastyle:off
  val dataFrameObj = new DataFrameOperations
  val dataSetObj = new DatasetOperations

  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("spark-day-3").setMaster("local")
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  // Read the input CSV file using Spark Session and create a DataFrame
  val csvFilePath = "/home/knoldus/spark-day-3/src/main/resources/D1.csv"
  val csvDataFrame = dataFrameObj.readCsvFile(csvFilePath, spark)
  csvDataFrame.show()

  // Total number of match played by each team as HOME TEAM
  val numberOfMatch = dataFrameObj.getHomeMatchesCount(csvDataFrame, spark)
  numberOfMatch.show()

  // Top 10 team with highest winning percentage
  val topTenWinningTeams = dataFrameObj.getTopTenWinningTeams(csvDataFrame, spark)
  topTenWinningTeams.show()


  //Convert the DataFrame created in Q1 to DataSet by using only following fields.
  val dataSetFromDataFrame = dataSetObj.convertDataFrameToDataSet(csvDataFrame, spark)
  dataSetFromDataFrame.show()

  // Total number of match played by each team
  val totalMatches = dataSetObj.getTotalMatchesForEachTeam(dataSetFromDataFrame, spark)
  totalMatches.show()

  // Top Ten team with highest wins
  val topTenTeams = dataSetObj.getTopTenTeamsWithHighestWin(dataSetFromDataFrame, spark)
  topTenTeams.show()

  //scalastyle:on

}
