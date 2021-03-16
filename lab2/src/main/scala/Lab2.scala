import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import scala.collection.mutable._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.operation.polygonize.Polygonizer
//import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
//import org.locationtech.geomesa.utils.geotools.SchemaBuilder

import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

// docker run -it --rm -v "`pwd`":/io spark-shell --packages com.uber:h3:3.0.4,\org.locationtech.geomesa:geomesa-spark-jts_2.11:2.4.1

object Lab2 {

  val geoToH3 = org.apache.spark.sql.functions.udf {
    (latitude: Double, longitude: Double, resolution: Int) =>
      H3Core.newInstance().geoToH3(latitude, longitude, resolution)
  }

  val polygonToH3 = org.apache.spark.sql.functions.udf {
    (geometry: Geometry, resolution: Int) =>
      var points: List[GeoCoord] = List()
      var holes: List[java.util.List[GeoCoord]] = List()
      if (geometry.getGeometryType == "Polygon") {
        points = List(
          geometry.getCoordinates.toList.map(coord =>
            new GeoCoord(coord.y, coord.x)
          ): _*
        )
      }
      H3Core.newInstance().polyfill(points, holes.asJava, resolution).toList
  }

  private def getConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
//        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
//        classOf[org.apache.spark.sql.types.StructType],
//        classOf[Array[org.apache.spark.sql.types.StructType]],
//        classOf[org.apache.spark.sql.types.StructField],
//        classOf[Array[org.apache.spark.sql.types.StructField]],

//        Class.forName("org.apache.spark.sql.types.StringType$"),
//        Class.forName("org.apache.spark.sql.types.LongType$"),
//        Class.forName("org.apache.spark.sql.types.BooleanType$"),
//        Class.forName("org.apache.spark.sql.types.DoubleType$"),
//        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
//        classOf[org.apache.spark.sql.catalyst.InternalRow],
//        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
//        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
//        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
//        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
//        classOf[org.apache.spark.util.collection.BitSet],
//        classOf[org.apache.spark.sql.types.DataType],
//        classOf[Array[org.apache.spark.sql.types.DataType]],
//        Class.forName("org.apache.spark.sql.types.NullType$"),
//        Class.forName("org.apache.spark.sql.types.IntegerType$"),
//        Class.forName("org.apache.spark.sql.types.TimestampType$"),
//        Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
//        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
//        Class.forName("scala.collection.immutable.Set$EmptySet$"),
//        Class.forName("scala.reflect.ClassTag$$anon$1"),
//        Class.forName("java.lang.Class"),
        classOf[com.uber.h3core.util.GeoCoord]
      )
    )
  }

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("testings")
      .config("spark.workers.show", "true")
//      .config("spark.driver.memory", "4g")
      .config(getConfig)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrator", "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
//      .config("spark.kryo.registrationRequired", "true")
      .config("spark.eventLog.enabled", "true")
      .config("spark.ui.prometheus.enabled", "true")
      .config("spark.yarn.executor.memoryOverhead", "1000")
//      .config("spark.executor.instances", "5")
//      .config("spark.executor.cores", "8")
//      .config("spark.executor.memory", "24g")
      .getOrCreate()
      .withJTS

  object GeoService {
    @transient lazy val geometryFactory = new GeometryFactory
  }

  val createGeometry =
    org.apache.spark.sql.functions.udf {
      (points: WrappedArray[WrappedArray[Row]]) =>
        val polygonizer = new Polygonizer
        points
          .map(
            _.map { case Row(lat: Double, lon: Double) => (lat, lon) }
              .map(a => new Coordinate(a._2, a._1))
          )
          .map(x => GeoService.geometryFactory.createLineString(x.toArray))
          .foreach(polygonizer.add)
        val collection = polygonizer.getGeometry
        (0 until collection.getNumGeometries).map(collection.getGeometryN)
    }

  def main(args: Array[String]) {
    import spark.implicits._
    initJTS(spark.sqlContext)

    val ds = spark.read
      .orc("s3://abs-tudelft-sbd20/us.orc")
      .select("id", "type", "tags", "lat", "lon", "nds", "members")
//    val ds = spark.read.orc("zuid-holland.orc").select("id", "type", "tags", "lat", "lon", "nds", "members")

    val nodes = ds
      .filter('type === "node")
      .select('id.as("node_id"), 'lat, 'lon)
      .map(x =>
        (
          x.getLong(0),
          x.getDecimal(1).doubleValue(),
          x.getDecimal(2).doubleValue()
        )
      )
      .toDF("node_id", "lat", "lon")

    nodes.cache() //This line will cause error if the data is much larger than our cluster memory
    val ways = ds
      .filter('type === "way")
      .select('id.as("way_id"), posexplode($"nds.ref"))
      .select('way_id, struct('pos, 'col.as("id")).as("node"))
      .join(nodes, $"node.id" === 'node_id)
      .groupBy('way_id)
      .agg(
        array_sort(
          collect_list(struct($"node.pos".as("pos"), struct($"lat", $"lon")))
        ).as("way")
      )
      .select($"way_id", $"way.col2".as("loc"))
    nodes.unpersist()

    val brewery = ds
      .filter(functions.size(ds("tags")) > 0)
      .select("id", "tags", "lat", "lon")
      .filter(
        $"tags" ("craft") === "brewery" || $"tags" (
          "buliding"
        ) === "brewery" || $"tags" ("industrial") === "brewery" || $"tags" (
          "microbrewery"
        ) === "yes" || $"tags" ("brewery") === "*"
      )
      .filter($"lat" !== 0)
      .select("lon", "lat") //filter out all brewery with location information

//  val city_center = city_relations.filter($"col" ("type") === "node") //the city center location(Not used)

    val city_relations = ds
      .filter(
        $"tags" ("boundary") === "administrative" && $"tags" (
          "admin_level"
        ) === 8 && $"type" === "relation"
      )
      .select(
        $"tags" ("name").as("name"),
        posexplode($"members")
      ) //select the city name and explode the "members"(geographic information)
    ds.unpersist()

    ways.cache() //This line will cause error if the data is much larger than our cluster memory
    val city_poly = city_relations
      .filter($"col" ("type") === "way")
      .join(ways, $"col.ref" === $"way_id")
      .groupBy('name)
      .agg(array_sort(collect_list(struct($"pos", $"loc"))).as("nodes"))
      .select($"name", $"nodes.loc")
      .withColumn("poly", createGeometry($"loc"))
      .filter(functions.size($"poly") > 0)
      .withColumn("p", explode($"poly"))
      .select("name", "p")
    city_relations.unpersist()
    ways.unpersist()
    //    val brewery_all_point = brewery.withColumn("point", st_point($"lon", $"lat")).select("point")

//    city_poly.show()
//    brewery.show()

    val city_poly_h3 = city_poly
      .withColumn("p", polygonToH3($"p", lit(7)))
      .withColumn("p", explode($"p"))
    city_poly.unpersist()
    val brewery_h3 =
      brewery.withColumn("p", geoToH3($"lat", $"lon", lit(7))).select("p")
    brewery.unpersist()

    city_poly_h3.cache()
    brewery_h3.cache()

    val res = brewery_h3
      .join(city_poly_h3, "p")
      .select("name")
      .map(x => (x.getString(0), 1))
      .rdd
      .reduceByKey(_ + _)
      .toDF("name", "count")
      .orderBy(desc("count"))

    res.show()
//    res.write.mode(SaveMode.Overwrite).format("orc")save("output.orc")
//    res.write.mode(SaveMode.Overwrite).format("orc")save("s3://aws-logs-805895093473-us-east-1/output.orc")
//    res.write.mode(SaveMode.Overwrite).format("orc")save("s3://aws-logs-515960624753-us-east-1/output.orc")//tyd

    res.foreach(println(_))

    spark.stop()
  }
}
