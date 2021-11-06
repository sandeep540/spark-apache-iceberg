
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hive.HiveCatalog
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}

import java.util
import java.util.UUID

object MyTestCode extends App {


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
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

  ds.show(5)
  println(ds.count())

  import org.apache.iceberg.{PartitionSpec, Schema, Table}
  import org.apache.iceberg.catalog.TableIdentifier
  import org.apache.iceberg.types.Types

  //Iceberg
  import org.apache.iceberg.hadoop.HadoopCatalog


  val namespace = "dbo"
  val tableName = "persontable"
  /*val catalog = new HiveCatalog()
  catalog.setConf(spark.sparkContext.hadoopConfiguration)

  val properties = new util.HashMap[String, String]();
  properties.put("warehouse", "")
  properties.put("uri", "")

  catalog.initialize("hive", properties) */

  import org.apache.iceberg.hadoop.HadoopCatalog

  val conf = new Configuration();
  val warehousePath = "/Users/sandeep/iceberg"
  val catalog = new HadoopCatalog(conf, warehousePath)

  val name = TableIdentifier.of(namespace, tableName)

  import org.apache.iceberg.hadoop.HadoopTables



  val tableschema = new Schema(
    Types.NestedField.optional(1, "id", Types.StringType.get()),
    Types.NestedField.optional(2, "first_name", Types.StringType.get()),
    Types.NestedField.optional(3, "last_name", Types.StringType.get()),
    Types.NestedField.optional(4, "email", Types.StringType.get()),
    Types.NestedField.optional(5, "gender", Types.StringType.get()),
    Types.NestedField.optional(6, "ip_address", Types.StringType.get())
  )

  val spec = PartitionSpec.builderFor(tableschema).identity("gender").build()
  //val logtable = catalog.createTable(name, tableschema, spec)

  val tables = new HadoopTables(conf)
  val table = tables.create(tableschema, spec, "/Users/sandeep/iceberg/person")

  import org.apache.iceberg.catalog.Catalog
  import org.apache.iceberg.hadoop.HadoopCatalog


  ds.write
    .format("iceberg")
    .mode("overwrite")
    .save(s"$namespace.$tableName")

}

