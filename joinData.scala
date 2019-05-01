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
CUISINE_DESCRIPTION,
INSPECTION_DATE,
ACTION,
CRITICAL_FLAG,
SCORE,
GRADE,
GRADE_DATE,
RECORD_DATE,
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
val fname = "hdfs:/user/yyl346/project/yelpdata.txt"
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
val MERGE_DF = sqlContext.sql("""
    SELECT *
    FROM YELP_DF, INSP_DF
    WHERE YELP_DF.address1 = INSP_DF.address
""")
MERGE_DF.persist
MERGE_DF.write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:/user/yyl346/project/merge_save")
