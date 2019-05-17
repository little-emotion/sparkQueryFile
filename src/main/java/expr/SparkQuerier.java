package expr;

import java.io.FileInputStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkQuerier {

  final static String TABLE_NAME = "file_table";
  private static Logger logger = LoggerFactory.getLogger(SparkQuerier.class);

  public static void main(String[] args) {
    Config config;
    if (args.length > 0) {
      try {
        FileInputStream fileInputStream = new FileInputStream(args[0]);
        config = new Config(fileInputStream);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Load config from {} failed, using default config", args[0]);
        config = new Config();
      }
    } else {
      config = new Config();
    }

    SparkQuerier querier = new SparkQuerier();
    querier.query(config);
  }

  public long query(Config config) {
    String filePath = config.FILE_PATH;
    SparkSession spark;
    Dataset<Row> rowDataset;
    switch (config.DATABASE) {
      case "TSFILE":
        // TODO tsfile-spark-connector
        TsfileSparkSession tsfileSparkSession = new TsfileSparkSession();
        spark = tsfileSparkSession.getSession();
        rowDataset = tsfileSparkSession.say3(filePath, spark);
        break;
      case "PARQUET":
        spark = SparkSession.builder().appName("spark-query").master("local").getOrCreate();
        spark.conf().set("spark.sql.parquet.enableVectorizedReader", "false");
        spark.conf().set("spark.sql.parquet.filterPushdown", "true");
        rowDataset = spark.read().parquet(filePath);
        break;
      case "ORC":
        spark = SparkSession.builder().appName("spark-query").master("local").getOrCreate();
        spark.conf().set("spark.sql.orc.filterPushdown", "true");
        spark.conf().set("spark.sql.orc.impl", "native");

        rowDataset = spark.read().orc(filePath);
        break;
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }

    rowDataset.createOrReplaceTempView(TABLE_NAME);

    long startTime = System.nanoTime();

    String sql = genSQL(config);
    logger.info("query sql is {}",sql);
    Dataset<Row> dataset = spark.sql(sql);
    dataset.foreach(l->{
      l.get(0);
    });
    Double totalTimeInMS = (System.nanoTime() - startTime) / 1000_000D;

    final long line = dataset.count();
    logger.info("query time: {} ms", totalTimeInMS);
    logger.info("query line: {} , speed {} lines/s", line,  1000.0 * line / totalTimeInMS);
    return System.currentTimeMillis() - startTime;
  }


  private static String genSQL(Config config){
    String field = config.FIELD;
    String tag = config.QUERY_TAG;

    String[] fieldArr = field.split(",");

    String sql = "select "+ genFieldName(tag, fieldArr[0], config);
    for(int i = 1; i<fieldArr.length;i++){
      sql+= ", "+genFieldName(tag, fieldArr[i], config);
    }
    sql+=" from  " + TABLE_NAME + " where ";

    String TIME_NAME = getTimeStrName(config);
    sql+=TIME_NAME + " >= " +config.START_TIME;
    sql+=" and " + TIME_NAME + " <= " +config.END_TIME;

    if(config.IS_WITH_VALUE_FILE){
      sql += " and "+genFieldName(tag, fieldArr[0], config)+ " <= 50000";
    }
    return sql;
  }

  public static String genFieldName(String tag, String field, Config config){
    if(config.DATABASE.equalsIgnoreCase("TSFILE")){
      return "`"+tag+"."+field+"`";
    }
    else {
      return field;
    }
  }

  private static String getTimeStrName(Config config){
    switch (config.DATABASE) {
      case "TSFILE":
        return "time";
      case "PARQUET":
        return "time";
      case "ORC":
        return "timestamp";
      default:
        throw new RuntimeException(config.DATABASE + " not supported");
    }
  }
}