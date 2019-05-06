// spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
val dfRDD = MERGE_DF.rdd.filter(row => row(13).toString != "").map(_.toSeq)
// (name+address+insp_date, grade)
val gradeKV = dfRDD.map(row => (row(2).toString + row(3).toString + row(9).toString, row(13).toString))

val gradeTableArray = gradeKV.collect
var myMap = scala.collection.mutable.Map.empty[String, String]
for (i <- gradeTableArray) {
    myMap(i._1) = i._2
}
def transfer(arr: Seq[Any]): Seq[Any] = {
    val key = arr(2).toString + arr(3).toString + arr(9).toString
    arr.updated(13, myMap.getOrElse(key, ""))
}

val m1 = MERGE_DF.rdd.map(_.toSeq).map(transfer(_))
m1.persist
// m1.map(row => (row(11), row(21), row(25))).take(250).foreach(println)
val addGradeRDD = m1.filter(row => row(13).toString != "").filter(
    row => !row(14).toString.contains("reopen") && !row(14).toString.contains("Non-operational")) // use this
addGradeRDD.persist
// (99 7th Ave S,12/19/2017,A)
//
// val sampleArr = addGradeRDD.filter(x => x(13) == "A").takeSample(false, 3000)
// val aRDD = sc.makeRDD(sampleArr)
// val bRDD = addGradeRDD.filter(x => x(13) == "B")
// val cRDD = addGradeRDD.filter(x => x(13) == "C")
// val correctGradeRDD = aRDD.union(bRDD).union(cRDD)


// def printFeature(feature: (Any, Array[Double])): String = {
//     val label = feature._1
//     var s = label.toString
//     var index = 0
//     for (i <- feature._2) {
//         index += 1
//         s += (" " + index + ":" + i.toString)
//     }
//     s
// }
//
// def mapTo01(arr: Array[Double], maxVal: Array[Double], minVal: Array[Double]): Array[Double] = {
//     for (i <- 0 to arr.length - 1) {
//         if (maxVal(i) == minVal(i)) {
//             arr(i) = arr(i)
//         } else {
//             arr(i) = (arr(i) - minVal(i)) / (maxVal(i) - minVal(i))
//         }
//
//     }
//     arr
// }

// --------------------------------------------------------------------------------
// val featureRDD = correctGradeRDD.map(
//     x => (gradeToInt(x(25).toString),
//     Array(x(1).toString.toDouble, // lat
//     x(2).toString.toDouble, // long
//     priceToInt(x(8).toString).toString.toDouble, // price
//     x(9).toString.toDouble, // rating
//     x(10).toString.toDouble, // review_count
//     dateToInt(x(21).toString).toString.toDouble) // insp_date
//     ++ foodToFeature(x(20).toString)
// ))
// val sample = featureRDD.take(1)(0)._2
// var arrMax = Array[Double]()
// for (i <- 0 to sample.length - 1) {
//     arrMax = arrMax :+ featureRDD.map(x => x._2(i).toString.toDouble).max.toDouble
// }
// var arrMin = Array[Double]()
// for (i <- 0 to sample.length - 1) {
//     arrMin = arrMin :+ featureRDD.map(x => x._2(i).toString.toDouble).min.toDouble
// }
// val theRDD = featureRDD.map(x => (x._1, mapTo01(x._2, arrMax, arrMin)))
//
// val libsvmRDD = theRDD.map(x => printFeature(x))
// libsvmRDD.take(10).foreach(println)
// libsvmRDD.saveAsTextFile("hdfs:/user/yyl346/project/e.log")


// // --------------------------------------------------------------------------------
// val featureRDD = correctGradeRDD.map(
//     x => (gradeToInt(x(25).toString),
//     Array(x(1).toString.toDouble, // lat
//     x(2).toString.toDouble, // long
//     priceToInt(x(8).toString).toString.toDouble, // price
//     x(9).toString.toDouble, // rating
//     x(10).toString.toDouble, // review_count
//     dateToInt(x(21).toString).toString.toDouble // insp_date
//     // scoreToInt(x(24).toString).toString.toDouble
//     )
// ))
// val sample = featureRDD.take(1)(0)._2
// var arrMax = Array[Double]()
// for (i <- 0 to sample.length - 1) {
//     arrMax = arrMax :+ featureRDD.map(x => x._2(i).toString.toDouble).max.toDouble
// }
// var arrMin = Array[Double]()
// for (i <- 0 to sample.length - 1) {
//     arrMin = arrMin :+ featureRDD.map(x => x._2(i).toString.toDouble).min.toDouble
// }
// val theRDD = featureRDD.map(x => (x._1, mapTo01(x._2, arrMax, arrMin)))
//
// val libsvmRDD = theRDD.map(x => printFeature(x))
// libsvmRDD.take(10).foreach(println)
// libsvmRDD.saveAsTextFile("hdfs:/user/yyl346/project/d.log")

// // --------------------------------------------------------------------------------
// val vioRDD = correctGradeRDD.map(x => (x(29).toString, x(24).toString, x(25).toString)).
//     filter(x => x._1 != "").
//     filter(x => x._2 != "null" && x._2.toInt >= 0).
//     filter(x => x._3 == "A" || x._3 == "B" || x._3 == "C")
//     // map(x => x._3).
//     // distinct.sortBy(_.toString).collect.foreach(println)
//     // take(240).foreach(println)
// val vioGroupRDD = vioRDD.map(x => (x._1, (x._2.toInt, 1))).
//     reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).
//     mapValues(x => x._1.toDouble / x._2.toDouble).
//     mapValues(x => (x, scoreToGrade(x)))
//
// vioGroupRDD.sortBy(x => x._1).collect.foreach(println)
// // --------------------------------------------------------------------------------
// val vioRDD = correctGradeRDD.map(x => (x(29).toString, x(24).toString, x(25).toString)).
//     filter(x => x._1 != "").
//     filter(x => x._2 != "null" && x._2.toInt >= 0).
//     filter(x => x._3 == "A" || x._3 == "B" || x._3 == "C")
//     // map(x => x._3).
//     // distinct.sortBy(_.toString).collect.foreach(println)
//     // take(240).foreach(println)
// // def vectorizeGrade(str: String): Array[Int] = {
// //     if (str == "A")
// //         Array(1,0,0)
// //     else if (str == "B")
// //         Array(0,1,0)
// //     else if (str == "C")
// //         Array(0,0,1)
// //     else
// //         Array(0,0,0)
// // }
// val vioGroupRDD = vioRDD.map(x => (x._1, vectorizeGrade(x._3))).
//     reduceByKey((a, b) => Array(a(0)+b(0), a(1)+b(1), a(2)+b(2))).
//     mapValues(arr => (arr(0).toDouble / arr.sum, arr(1).toDouble / arr.sum, arr(2).toDouble / arr.sum))
//
// vioGroupRDD.sortBy(x => x._1).collect.foreach(println)
// just for reference
//  0|-- food: string (nullable = true)
//  1|-- latitude: double (nullable = true)
//  2|-- longitude: double (nullable = true)
//  3|-- display_phone: string (nullable = true)
//  4|-- distance: double (nullable = true)
//  5|-- id: string (nullable = true)
//  6|-- is_closed: boolean (nullable = true)
//  7|-- yelpname: string (nullable = true)
//  8|-- price: string (nullable = true)
//  9|-- rating: double (nullable = true)
// 10|-- review_count: integer (nullable = true)
// 11|-- address1: string (nullable = true)
// 12|-- city: string (nullable = true)
// 13|-- country: string (nullable = true)
// 14|-- state: string (nullable = true)
// 15|-- zip_code: integer (nullable = true)
// 16|-- NAME: string (nullable = true)
// 17|-- BORO: string (nullable = true)
// 18|-- ADDRESS: string (nullable = true)
// 19|-- ZIPCODE: integer (nullable = true)
// 20|-- CUISINE_DESCRIPTION: string (nullable = true)
// 21|-- INSPECTION_DATE: string (nullable = true)
// 22|-- ACTION: string (nullable = true)
// 23|-- CRITICAL_FLAG: string (nullable = true)
// 24|-- SCORE: string (nullable = true)
// 25|-- GRADE: string (nullable = true)
// 26|-- GRADE_DATE: string (nullable = true)
// 27|-- RECORD_DATE: string (nullable = true)
// 28|-- INSPECTION_TYPE: string (nullable = true)
// 29|-- VIOLATION_CODE: string (nullable = true)
// 30|-- VIOLATION_DESCRIPTION: string (nullable = true)
