import MyTestCode.{ds, namespace, tableName}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}


object IcebergDataset extends App {

  System.setProperty("hadoop.home.dir","/Users/sandeep/iceberg1/example" )
  val conf = new SparkConf()
  conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  conf.set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
  conf.set("spark.sql.catalog.spark_catalog.type","hive")
  conf.set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
  conf.set("spark.sql.catalog.local.type","hadoop")
  conf.set("spark.sql.catalog.local.warehouse","data/warehouse")


  val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

  import spark.implicits._

  val ds: Dataset[Person] = spark.read
    .format("csv")
    .option("header", "true") //first line in file has headers
    .option("charset", "UTF8")
    .option("delimiter",",")
    .schema(Encoders.product[Person].schema)
    .load("data.csv")
    .as[Person]

  ds.createOrReplaceTempView("persontable")

  import org.apache.iceberg.{PartitionSpec, Schema, Table}
  import org.apache.iceberg.catalog.TableIdentifier
  import org.apache.iceberg.types.Types

  //Iceberg
  import org.apache.iceberg.hadoop.HadoopCatalog


  val namespace = "dbo"
  val tableName = "person"

  val conf1 = new Configuration();
  val warehousePath = "/Users/sandeep/iceberg"
  val catalog = new HadoopCatalog(conf1, warehousePath)

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

  val tables = new HadoopTables(conf1)
  val table = tables.create(tableschema, spec, "/Users/sandeep/iceberg1/person")

  ds.write
    .format("iceberg")
    .mode("overwrite")
    .save(s"$namespace.$tableName")

}
