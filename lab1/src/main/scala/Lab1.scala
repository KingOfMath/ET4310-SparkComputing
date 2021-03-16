package example

import org.apache.spark.sql.Column
import java.sql.Timestamp
import java.io.Serializable;
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._

import scala.collection.mutable._

object Lab1 {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("testings")
      .master("local")
      .config("spark.workers.show", "True")
      .getOrCreate
  val logger: Logger = Logger.getLogger("Application")

  //Define a Point class, represting a geographic point
  case class Point(var x: Double, var y: Double) extends java.io.Serializable {
    def getPoint: Point = Point.apply(x, y)
  }

  //Determine whether a point (p) is in the polygon (pts).
  def isInPloyin(p: Point, pts: List[Point]): Int = {
    var intersectionp = 0
    for (i <- pts.indices) {

      val p1 = pts(i)
      val p2 = pts((i + 1) % pts.size)

      if (p.y >= Math.min(p1.y, p2.y) && p.y < Math.max(p1.y, p2.y))
        if (((p.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < p.x)
          intersectionp += 1
    }
    intersectionp % 2
  } //1-inner, 0-outter
  //https://blog.csdn.net/k_wzzc/article/details/82779291

  //Remove the points which does not form a polygon
  def rmExtPoint(plist: List[Point], angle: Int): List[Point] = {
    val output = new ListBuffer[Point]()
    for (i <- 0 until plist.size) {
      val p1 = plist((i) % plist.size)
      val p2 = plist((i + 1) % plist.size)
      val p3 = plist((i + 2) % plist.size)

      var diff_angle = Math.atan((p1.y - p2.y) / (p1.x - p2.x)) - Math.atan(
        (p3.y - p2.y) / (p3.x - p2.x)
      )
      if (diff_angle > Math.PI / 2) diff_angle = diff_angle - Math.PI
      if (diff_angle < (-Math.PI / 2)) diff_angle = diff_angle + Math.PI
      diff_angle = Math.abs(diff_angle)
      if (diff_angle < Math.PI / 360 * angle || diff_angle.isNaN) {} else {
        output.append(p2)
      }
    }
    output.toList
  }

  //Sort the points
  def tdimSort(plist: List[Point]): List[Point] = {
    val orthocenter =
      Point(plist.map(_.x).sum / plist.size, plist.map(_.y).sum / plist.size)
    val x = orthocenter.x
    val y = orthocenter.y
    val voatan2 = Math.atan2(y, x)
    plist
      .map(p => {
        val theta: Double = voatan2 - Math.atan2(p.y - y, p.x - x)
        (p, theta)
      })
      .sortBy(_._2)
      .map(_._1)
  } //https://blog.csdn.net/k_wzzc/article/details/83066147

  def getContainResult(
      brewery: Dataset[(Long, (Double, Double))],
      city_boundary: RDD[(String, List[Point])]
  ): RDD[(Long, String, Int)] = {
    brewery.cache()
    city_boundary.cache()
    val id_calculation_table = brewery.rdd
      .cartesian(city_boundary)
      .map(x => (x._1._1, x._2._1, Point(x._1._2._1, x._1._2._2), x._2._2))
    val id_calculation_result = id_calculation_table.map(x =>
      (
        x._1,
        x._2, {
          var intersectionp = 0
          for (i <- x._4.indices) {
            val p1 = x._4(i)
            val p2 = x._4((i + 1) % x._4.size)
            if (x._3.y >= Math.min(p1.y, p2.y) && x._3.y < Math.max(p1.y, p2.y))
              if (
                ((x._3.y - p1.y) * (p2.x - p1.x) / (p2.y - p1.y) + p1.x) < x._3.x
              )
                intersectionp += 1
          }
          intersectionp % 2
        }
      )
    )
    val breweery_city_id = id_calculation_result.filter(x => x._3 == 1)
    breweery_city_id
  }

  def main(args: Array[String]) {
    val REMOVE_POLYGON_POINT_ANGLGE = 10
    val NUMBER_TOP_CITY_OUTPUT = 25

    var polygon_point_angle = REMOVE_POLYGON_POINT_ANGLGE

    import spark.implicits._
    var dataBaseLoc = "zuid-holland.orc"; //Default location
    if (args.length >= 1) {
      if (args(0) != "default") {
        dataBaseLoc = args(0)
      }
    }
    if (args.length >= 2) {
      polygon_point_angle = args(1).toInt
    }
    var ds = spark.emptyDataFrame;
    try {
      ds = spark.read.orc(dataBaseLoc);
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        logger.info("the database does not exist.")
        System.exit(-1)
    }

    // val ds = spark.read.orc("zuid-holland.orc");
    val tags = ds
      .filter(functions.size(ds("tags")) > 0)
      .select(
        "id",
        "tags",
        "lat",
        "lon"
      ) //Filter out empty-tag points and select what we concern
    val tag_col = tags("tags")
    val brewery = tags.filter(
      tag_col("craft") === "brewery" || tag_col(
        "buliding"
      ) === "brewery" /*|| tag_col("man_made")==="works"*/ || tag_col(
        "industrial"
      ) === "brewery" || tag_col("microbrewery") === "yes" || tag_col(
        "brewery"
      ) === "*"
      /*|| tag_col("shop") === "alcohol" || tag_col("shop") === "beverages"*/
    ) //filter out all breweries
    val brewery_all_with_lat =
      brewery.filter(
        $"lat" !== 0
      ) //filter out all brewery with location information

    //polygon method
    val boundary = ds.filter(
      $"tags" ("boundary") === "administrative" && $"tags" (
        "admin_level"
      ) === 8 && $"type" === "relation"
    ) //find the city-level geographic information
    val city_boundary = boundary.select(
      $"tags" ("name"),
      functions.explode($"members")
    ) //select the city name and explode the "members"(geographic information)
    val city_boundary_ways =
      city_boundary.filter(
        $"col" ("type") === "way"
      ) //remove the city center location
    val city_boundary_city_center = city_boundary.filter(
      $"col" ("type") === "node"
    ) //the city center location(Not used)
    val city_boundary_ways_long = city_boundary_ways.select(
      "tags[name]",
      "col.ref"
    ) //select city name and a list of boundary ways

    val ways =
      ds.select($"id", $"nds")
        .filter("type='way'") //find all ways in OpenStreetMap
    val nodes =
      ds.filter("type='node'")
        .select($"id", $"lat", $"lon") //find all nodes in OpenStreetMap

    val city_boundary_ways_id = city_boundary_ways_long.withColumn(
      "index",
      functions.monotonically_increasing_id
    ) //add index to the boundary
    val city_ways_id_grouped = city_boundary_ways_id
      .join(ways)
      .where($"ref" === $"id")
      .select("tags[name]", "index", "nds")
      .orderBy(functions.asc("index")) //restore the order of the boundary ways
    val city_ways_id_raw =
      city_ways_id_grouped.select($"tags[name]", functions.explode($"nds.ref"))

    val city_ways_id = city_ways_id_raw.withColumn(
      "index",
      functions.monotonically_increasing_id
    ) //add index to the boundary
    val city_nodes_id = city_ways_id
      .join(nodes)
      .where($"col" === $"id")
      .select("tags[name]", "index", "lat", "lon")
      .orderBy(functions.asc("index")) //restore the order
    //city_nodes_id.cache()
    val city_points_id = city_nodes_id.map(x =>
      (
        x.getString(0),
        Point(x.getDecimal(2).doubleValue, x.getDecimal(3).doubleValue)
      )
    ) //Form points
    val city_points_id_grouped = city_points_id.rdd.groupByKey()
    //because we used class Point here, so store the data to RDD to keep the Point memory format
    //Dataframe does not keep the "Point" class structure

    val city_boundary_id = city_points_id_grouped.map(x =>
      (x._1, rmExtPoint(x._2.toList, polygon_point_angle))
    ) //filter unnecessary points
    //val city_boundary_id = city_points_id_grouped.map(x => (x._1, x._2.toList))//for no filter use

    val id_brewery_to_process = brewery_all_with_lat.map(x =>
      (x.getLong(0), (x.getDecimal(2).doubleValue, x.getDecimal(3).doubleValue))
    ) //prepear the brewery data

    val breweery_city_id =
      getContainResult(id_brewery_to_process, city_boundary_id)
    val breweery_city_id_counter = breweery_city_id.map(x => (x._2, x._3))
    val brewery_add_city_id =
      breweery_city_id_counter.reduceByKey((x, y) => x + y)
    val brewery_city_counter_ordered_id =
      brewery_add_city_id.top(NUMBER_TOP_CITY_OUTPUT)(Ordering.by(e => e._2))
    logger.info(
      brewery_city_counter_ordered_id
        .take(NUMBER_TOP_CITY_OUTPUT)
        .mkString("\n")
    ) //print the top 25 cities
  }
}
