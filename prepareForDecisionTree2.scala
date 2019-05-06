// --------------------------------------------------------------------------------
// (viocode, score, grade)
val vioRDD = correctGradeRDD.map(x => (x(10).toString, x(12).toString, x(13).toString)).
    filter(x => x._1 != "").
    filter(x => x._2 != "null" && x._2.toInt >= 0).
    filter(x => x._3 == "A" || x._3 == "B" || x._3 == "C")
    // map(x => x._3).
    // distinct.sortBy(_.toString).collect.foreach(println)
    // take(240).foreach(println)
val vioGroupRDD = vioRDD.map(x => (x._1, (x._2.toInt, 1))).
    reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).
    mapValues(x => x._1.toDouble / x._2.toDouble).
    mapValues(x => (x, scoreToGrade(x)))

vioGroupRDD.sortBy(x => x._1).collect.foreach(println)
// --------------------------------------------------------------------------------
val vioRDD = correctGradeRDD.map(x => (x(10).toString, x(12).toString, x(13).toString)).
    filter(x => x._1 != "").
    filter(x => x._2 != "null" && x._2.toInt >= 0).
    filter(x => x._3 == "A" || x._3 == "B" || x._3 == "C")
    // map(x => x._3).
    // distinct.sortBy(_.toString).collect.foreach(println)
    // take(240).foreach(println)
def vectorizeGrade(str: String): Array[Int] = {
    if (str == "A")
        Array(1,0,0)
    else if (str == "B")
        Array(0,1,0)
    else if (str == "C")
        Array(0,0,1)
    else
        Array(0,0,0)
}
val vioGroupRDD = vioRDD.map(x => (x._1, vectorizeGrade(x._3))).
    reduceByKey((a, b) => Array(a(0)+b(0), a(1)+b(1), a(2)+b(2))).
    mapValues(arr => (arr(0).toDouble / arr.sum, arr(1).toDouble / arr.sum, arr(2).toDouble / arr.sum))

vioGroupRDD.sortBy(x => x._1).collect.foreach(println)
