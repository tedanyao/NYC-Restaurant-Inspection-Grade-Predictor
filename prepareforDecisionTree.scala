// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
val file = "hdfs:/user/yyl346/project/restaurant_title.csv"
val df = sqlContext.read.format("csv").option("header", "true").option("inferschema", "true").load(file)
df.printSchema

// 1. filter out grade==""
// 2. make a key for grouping food category: (name+address, food_id)
val correctGradeRDD = MERGE_DF.rdd
val sampleA = correctGradeRDD.filter(x => x(13) == "A").takeSample(false, 3747)
val sampleB = correctGradeRDD.filter(x => x(13) == "B").takeSample(false, 3747)
val aRDD = sc.makeRDD(sampleA)
val bRDD = sc.makeRDD(sampleB)
val cRDD = correctGradeRDD.filter(x => x(13) == "C")
val correctGradeRDD = aRDD.union(bRDD).union(cRDD).filter(x => x(6) != null).filter(x => x(12) != null)
val correctGradeRDD = correctGradeRDD

// -----------------------------------------------------
val featureRDD = correctGradeRDD.map(
    x => (gradeToInt(x(13).toString),
    Array(x(4).toString.toDouble, // lat
    x(5).toString.toDouble, // long
    priceToInt(x(6).toString).toString.toDouble, // price
    x(7).toString.toDouble, // rating
    x(8).toString.toDouble, // review_count
    dateToInt(x(9).toString).toString.toDouble, // insp_date
    scoreToInt(x(12).toString).toString.toDouble
    )
))
featureRDD.take(60)

val sample = featureRDD.take(1)(0)._2
var arrMax = Array[Double]()
for (i <- 0 to sample.length - 1) {
    arrMax = arrMax :+ featureRDD.map(x => x._2(i).toString.toDouble).max.toDouble
}
var arrMin = Array[Double]()
for (i <- 0 to sample.length - 1) {
    arrMin = arrMin :+ featureRDD.map(x => x._2(i).toString.toDouble).min.toDouble
}
val theRDD = featureRDD.map(x => (x._1, mapTo01(x._2, arrMax, arrMin)))

val libsvmRDD = theRDD.map(x => printFeature(x))
libsvmRDD.take(10).foreach(println)
libsvmRDD.saveAsTextFile("hdfs:/user/yyl346/project/d.log")
