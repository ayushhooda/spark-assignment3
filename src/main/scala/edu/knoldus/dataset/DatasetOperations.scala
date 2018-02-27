package edu.knoldus.dataset

import org.apache.spark.sql.{DataFrame, Dataset}

class DatasetOperations {

  def convertDataFrameToDataSet(df: DataFrame): Dataset[DataSetTable] = {
    df.as[DataSetTable]
  }

  def getTotalMatchesForEachTeam(ds: Dataset[DataSetTable]): Dataset[(String, Int)] = {

  }

  def getTopTenTeamsWithHighestWin(ds: Dataset[DataSetTable]): Dataset[(String, Int)] = {

  }
}

