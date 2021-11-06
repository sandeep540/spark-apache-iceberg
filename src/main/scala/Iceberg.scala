
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}

object Iceberg extends App {

  System.setProperty("hadoop.home.dir","/Users/sandeep/iceberg1/example" )
  val conf = new SparkConf()
  conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  conf.set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
  conf.set("spark.sql.catalog.spark_catalog.type","hive")
  conf.set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
  conf.set("spark.sql.catalog.local.type","hadoop")
  conf.set("spark.sql.catalog.local.warehouse","data/warehouse")


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Iceberg App")
    .getOrCreate()

  import spark.implicits._

  val ds: Dataset[Person] = spark.read
    .format("csv")
    .option("header", "true") //first line in file has headers
    .option("charset", "UTF8")
    .option("delimiter",",")
    .schema(Encoders.product[Person].schema)
    .load("data.csv")
    .as[Person]

  ds.createOrReplaceTempView("inputTable")

  /*
  CREATE TABLE local.default.t1 (id bigint, data string) USING iceberg;
  INSERT INTO local.default.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
   */

  spark.sql("CREATE TABLE sample (id String, value String) USING iceberg")

  spark.sql("INSERT INTO sample VALUES ('1', 'a'), ('2', 'b'), ('3', 'c')")

  //spark.sql("CREATE TABLE person (id String, first_name String, last_name String, email String, gender String, ip_address String) USING iceberg")

  //spark.sql("INSERT INTO TABLE default.person SELECT id, first_name, last_name, email, gender, ip_address FROM inputTable")

  //spark.sql("CREATE or REPLACE TABLE person USING iceberg AS SELECT * FROM inputTable");


  //
  //spark.sql("INSERT INTO TABLE prod.db.sample SELECT id, first_name, last_name, email, gender, ip_address FROM inputTable")

 /* ds.write
    .format("iceberg")
    .mode("overwrite")
    .save("/Users/sandeep/iceberg1/example")
*/
}
final case class Person (id: String, first_name: String, last_name: String, email: String, gender: String, ip_address: String)