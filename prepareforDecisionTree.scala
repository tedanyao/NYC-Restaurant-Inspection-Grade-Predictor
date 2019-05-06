// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
// val file = "hdfs:/user/yyl346/project/restaurant.csv"
// val df = sqlContext.read.format("csv").option("header", "true").option("inferschema", "true").load(file)
// df.printSchema
// grade
// A: 7526
// B: 370
// C: 49


// 1. filter out grade==""
// 2. make a key for grouping food category: (name+address, food_id)
val sampleA = addGradeRDD.filter(x => x(13) == "A").takeSample(false, 3862) // 75349
val sampleB = addGradeRDD.filter(x => x(13) == "B").takeSample(false, 3862) // 12082
val aRDD = sc.makeRDD(sampleA)
val bRDD = sc.makeRDD(sampleB)
val cRDD = addGradeRDD.filter(x => x(13) == "C") // 3862
val correctGradeRDD = aRDD.union(bRDD).union(cRDD).filter(x => x(6) != null).filter(x => x(12) != null)


// -----------------------------------------------------
// saving features to libsvm format
val featureRDD = correctGradeRDD.map(
    x => (gradeToInt(x(13).toString),
    Array(x(4).toString.toDouble, // lat
    x(5).toString.toDouble, // long
    priceToInt(x(6).toString).toString.toDouble, // price
    x(7).toString.toDouble, // rating
    x(8).toString.toDouble, // review_count
    dateToInt(x(9).toString).toString.toDouble, // insp_date
    vioToInt(x(10).toString).toDouble // vio_count
    // scoreToInt(x(12).toString).toString.toDouble
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
libsvmRDD.saveAsTextFile("hdfs:/user/yyl346/project/features.log")
