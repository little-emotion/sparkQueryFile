package expr

import org.apache.iotdb.tsfile._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TsfileSparkSession{
  def getSession():SparkSession={
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark
  }

  def say3(filePath: String, session: SparkSession): Dataset[Row] ={
    //read data in TsFile and create a table
    val df = session.read.tsfile(filePath)
    df.createOrReplaceTempView(SparkQuerier.TABLE_NAME)
    df
  }

}
