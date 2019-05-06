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

def sameName(a: String, b: String): Boolean = {
    var arrA = a.split("\\s+")
    var arrB = b.split("\\s+")
    if (arrA.length > arrB.length) {
        arrA = b.split("\\s+")
        arrB = a.split("\\s+")
    }
    var matchNum = 0
    for (i <- 0 to arrA.length - 1) {
        var found = 0
        var k = 0
        var j = 0
        while (j < arrB.length && found == 0) {
            if (arrA(i) != "" && arrB(j) != "" && arrA(i) == arrB(j)) {
                found = 1
                k = j
                matchNum += 1
            }
            j += 1
        }
        if (found == 1) {
            arrB(k) = ""
        }
    }
    val score = arrA.length * 0.8
    if (matchNum >= score.toInt) {
        true
    } else {
        false
    }
}
sqlContext.udf.register("sameName", (a:String, b:String) => sameName(a,b))

val MERGE_DF = sqlContext.sql("""
    SELECT DISTINCT PHONE, display_phone, NAME, ADDRESS, latitude, longitude,
    price, rating, review_count, INSPECTION_DATE, VIOLATION_CODE,
    address1, SCORE, GRADE, INSPECTION_TYPE, CUISINE_DESCRIPTION,
    CRITICAL_FLAG, yelpname,
    city, country, state, zip_code, BORO, ZIPCODE
    FROM YELP_DF, INSP_DF
    WHERE (correctPhone(PHONE) = correctPhone(display_phone) AND address1 = ADDRESS)
""")

val MERGE_DF = sqlContext.sql("""
    SELECT DISTINCT PHONE, display_phone, NAME, ADDRESS, latitude, longitude,
    price, rating, review_count, INSPECTION_DATE, VIOLATION_CODE,
    address1, SCORE, GRADE, INSPECTION_TYPE, CUISINE_DESCRIPTION,
    CRITICAL_FLAG, yelpname,
    city, country, state, zip_code, BORO, ZIPCODE
    FROM YELP_DF, INSP_DF
    WHERE (correctPhone(PHONE) = correctPhone(display_phone) AND address1 = ADDRESS)
    OR (sameName(NAME, yelpname) AND address1 = ADDRESS)
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

val myRDD = MERGE_DF.withColumn("violation_count", lit(1)).rdd.map(x => (x(0).toString, x))
myRDD.persist

def myString(a: Any): String = {
    if (a == null) {
        ""
    } else {
        a.toString
    }
}
import org.apache.spark.sql._

def removeComma(a: Row): Row = {
    Row(
    myString(a(0)).replace(',','/'),
    myString(a(1)).replace(',','/'),
    myString(a(2)).replace(',','/'),
    myString(a(3)).replace(',','/'),
    myString(a(4)).replace(',','/'),
    myString(a(5)).replace(',','/'),
    myString(a(6)).replace(',','/'),
    myString(a(7)).replace(',','/'),
    myString(a(8)).replace(',','/'),
    myString(a(9)).replace(',','/'),
    myString(a(10)).replace(',','/'),
    myString(a(11)).replace(',','/'),
    myString(a(12)).replace(',','/'),
    myString(a(13)).replace(',','/'),
    myString(a(14)).replace(',','/'),
    myString(a(15)).replace(',','/'),
    myString(a(16)).replace(',','/'),
    myString(a(17)).replace(',','/'),
    myString(a(18)).replace(',','/'),
    myString(a(19)).replace(',','/'),
    myString(a(20)).replace(',','/'),
    myString(a(21)).replace(',','/'),
    myString(a(22)).replace(',','/'),
    myString(a(23)).replace(',','/'),
    myString(a(24)).replace(',','/'))
}

def compareDate(a: String, b: String): Boolean = {
    // MM/DD/YY
    val A = a.split('/')
    val B = b.split('/')
    if (A(2) > B(2)) {
        true
    } else if (A(2) < B(2)) {
        false
    } else if (A(0) > B(0)) {
        true
    } else if (A(0) < B(0)) {
        false
    } else if (A(1) > B(1)) {
        true
    } else if (A(1) < B(1)) {
        false
    } else {
        true
    }
}

def violationMerge(a: Row, b: Row): Row = {
    val vioa = a(10).toString
    val viob = b(10).toString
    val newone = vioa + "|" + viob
    val newcount = a(24).toString.toInt + b(24).toString.toInt
    val aIsBigger = compareDate(a(9).toString, b(9).toString)
    var insp = b(9)
    var score = b(12)
    var grade = b(13)
    if (aIsBigger) {
        insp = a(9)
        score = a(12)
        grade = a(13)
    }
    Row(
    a(0),
    a(1),
    a(2),
    a(3),
    a(4),
    a(5),
    a(6),
    a(7),
    a(8),
    insp, // insp
    newone, // violation code
    a(11),
    score, // score
    grade, // grade
    a(14),
    a(15),
    a(16),
    a(17),
    a(18),
    a(19),
    a(20),
    a(21),
    a(22),
    a(23),
    newcount)
}

val my2RDD = myRDD.map(x => (x._1, removeComma(x._2))).reduceByKey((a,b) => violationMerge(a,b))
val outRDD = my2RDD.map(x => x._2)
outRDD.persist
// outRDD.take(3)


outRDD.saveAsTextFile("hdfs:/user/yyl346/project/restaurant")
