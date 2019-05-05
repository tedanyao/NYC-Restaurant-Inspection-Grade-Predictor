// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0


val file = "hdfs:/user/yyl346/project/DOHMH_New_York_City_Restaurant_Inspection_Results.csv"

val df = sqlContext.read.format("csv").option("header", "true").option("inferschema", "true").load(file)
var newDF = df
  for(col <- df.columns){
    newDF = newDF.withColumnRenamed(col,col.replaceAll("\\s", "_"))
  }
newDF.show
newDF.printSchema

newDF.registerTempTable("INSPECTION")

val INSP_DF = sqlContext.sql("""
SELECT initCap(lower(DBA)) AS NAME,
BORO,
CONCAT(initCap(lower(BUILDING)), ' ', initCap(lower(STREET))) AS ADDRESS,
ZIPCODE,
PHONE,
CUISINE_DESCRIPTION,
INSPECTION_DATE,
ACTION,
CRITICAL_FLAG,
SCORE,
GRADE,
INSPECTION_TYPE,
VIOLATION_CODE,
VIOLATION_DESCRIPTION
FROM INSPECTION
""")

INSP_DF.show
INSP_DF.printSchema
INSP_DF.persist

INSP_DF.registerTempTable("INSP_DF")

// =======================================================================
val fname = "hdfs:/user/yyl346/project/total.txt"
val df = sqlContext.read.json(fname)

df.registerTempTable("TEMP1")
val ddf = sqlContext.sql("""
    SELECT explode(businesses)
    FROM TEMP1
""")

val temp2 = ddf.select(
// 'col.getItem("alias") as 'alias,
// 'col.getItem("categories") as 'categories,
'col.getItem("categories").getItem("title") as 'food,
// 'col.getItem("categories").getItem("alias") as 'food_alias,
// 'col.getItem("coordinates") as 'coordinates,
'col.getItem("coordinates").getItem("latitude") as 'latitude,
'col.getItem("coordinates").getItem("longitude") as 'longitude,
'col.getItem("display_phone") as 'display_phone,
'col.getItem("distance") as 'distance,
'col.getItem("id") as 'id,
'col.getItem("is_closed") as 'is_closed,
'col.getItem("name") as 'yelpname,
// 'col.getItem("phone") as 'phone,
'col.getItem("price") as 'price,
'col.getItem("rating") as 'rating,
'col.getItem("review_count") as 'review_count,
// 'col.getItem("transactions") as 'transactions,
'col.getItem("location").getItem("address1") as 'address1,
// 'col.getItem("location").getItem("address2") as 'address2,
// 'col.getItem("location").getItem("address3") as 'address3,
'col.getItem("location").getItem("city") as 'city,
'col.getItem("location").getItem("country") as 'country,
'col.getItem("location").getItem("state") as 'state,
'col.getItem("location").getItem("zip_code") as 'zip_code
).distinct

temp2.registerTempTable("TEMP2")
val YELP_DF = sqlContext.sql("""
    SELECT *
    FROM TEMP2
""")
YELP_DF.registerTempTable("YELP_DF")

// YELP_DF.write.format("com.databricks.spark.csv").save("hdfs:/user/yyl346/project/yelp_save")
// =======================================================================
// profiling
def correctPhone(a: String): String = {
    var s = ""
    for (ch <- a) {
        if (ch >= '0' && ch <= '9')
            s =  s + ch.toString
    }
    s
}
sqlContext.udf.register("correctPhone", (a:String) => correctPhone(a))


val MERGE_DF = sqlContext.sql("""
    SELECT DISTINCT PHONE, display_phone, NAME, ADDRESS, latitude, longitude,
    price, rating, review_count, INSPECTION_DATE, VIOLATION_CODE,
    address1, SCORE, GRADE, INSPECTION_TYPE, CUISINE_DESCRIPTION,
    ACTION, CRITICAL_FLAG, yelpname,
    city, country, state, zip_code, BORO, ZIPCODE
    FROM YELP_DF, INSP_DF
    WHERE correctPhone(PHONE) = correctPhone(display_phone) AND address1 = ADDRESS
""")
MERGE_DF.persist
// MERGE_DF.write.format("com.databricks.spark.csv").save("hdfs:/user/yyl346/project/merge_save")

def samePhone(a: String, b: String): Boolean = {
    if (a == "" || b == "")
        false
    else {
        var s = ""
        for (ch <- b) {
            if (ch >= '0' && ch <= '9')
                s =  s + ch.toString
        }
        s == a
    }
}
import org.apache.spark.sql._
def violationMerge(a: Row, b: Row): Row = {
    val vioa = a(10).toString
    val viob = b(10).toString
    val newone = vioa + "|" + viob
    val newcount = a(25).toString.toInt + b(25).toString.toInt
    Row(a(0),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),newone,
    a(11),a(12),a(13),a(14),a(15),a(16),a(17),a(18),
    a(19),a(20),a(21),a(22),a(23),a(24), newcount)
}

val myRDD = MERGE_DF.withColumn("violation_count", lit(1)).rdd.map(x => (x(0).toString, x))

val my2RDD = myRDD.reduceByKey((a,b) => violationMerge(a,b))
val outRDD = my2RDD.map(x => x._2)
outRDD.persist
// val out2RDD = outRDD.map(x =>
//         x(0).toString + ","
//         + x(1).toString + ","
//         + x(2).toString + ","
//         + x(3).toString + ","
//         + x(4).toString + ","
//         + x(5).toString + ","
//         + priceToInt(x(6).toString) + ","
//         + x(7).toString + ","
//         + x(8).toString + ","
//         + x(9).toString + ","
//         + x(10).toString + ","
//         + x(11).toString + ","
//         + x(12).toString + ","
//         + x(13).toString + ","
//         + x(14).toString + ","
//         + x(15).toString + ","
//         + x(16).toString + ","
//         + x(17).toString + ","
//         + x(18).toString + ","
//         + x(19).toString + ","
//         + x(20).toString + ","
//         + x(21).toString + ","
//         + x(22).toString + ","
//         + x(23).toString + ","
//         + x(24).toString + ","
//         + x(25).toString + ","
//         + x(26).toString
// )
// out2RDD.take(3)
outRDD.saveAsTextFile("hdfs:/user/yyl346/project/merge_restaurant")
