// Remediation: if you have multiple violations taken into account
// P(A|violations) = sum(P(vio) * P(A|vio)) for vio in [all violations in a restaurant]
// predict max(P(A|violations), P(B|violations), P(C|violations))
val totalcount = vioRDD.count // 9434
// P(A|vio)
val vioGroupRDD = vioRDD.map(x => (x._1, vectorizeGrade(x._3))).
    reduceByKey((a, b) => Array(a(0)+b(0), a(1)+b(1), a(2)+b(2))).
    mapValues(arr => ((arr(0).toDouble / totalcount, arr(1).toDouble / totalcount, arr(2).toDouble / totalcount), maxProbGrade(arr)))
vioGroupRDD.sortBy(x => x._1).collect.foreach(println)

val vioTable = vioGroupRDD.sortBy(x => x._1).map(x => (x._1, x._2._1)).collect
var probMap = scala.collection.mutable.Map.empty[String, Tuple3[Double, Double, Double]]
for (i <- vioTable) {
    probMap(i._1) = i._2
}


val file = "hdfs:/user/yyl346/project/restaurant.csv"
val restaurantDF = sqlContext.read.format("csv").option("header", "true").option("inferschema", "true").load(file)
val restaurantRDD = restaurantDF.rdd
// (name, ref grade, violations)
def estimateGrade(vios: String): String = {
    val arr = vios.split('|')
    var probA = 0.0
    var probB = 0.0
    var probC = 0.0
    for (v <- arr) {
        probA += probMap.getOrElse(v, (0.0, 0.0, 0.0))._1
        probB += probMap.getOrElse(v, (0.0, 0.0, 0.0))._2
        probC += probMap.getOrElse(v, (0.0, 0.0, 0.0))._3
    }
    if (probA >= probB && probA >= probC) {
        "A"
    } else if (probB >= probA && probB >= probC) {
        "B"
    } else {
        "C"
    }
}
val resRDD = restaurantRDD.filter(x => x(10) != "").map(x => (x(2), x(13), x(10))) // .take(10).foreach(println)
val rrRDD = resRDD.map(x => (x._1, x._2, x._3, estimateGrade(x._3.toString)))

// error = 23.6% (1871/7942)
// accuracy = 76.4%
val refgradeRDD = rrRDD.filter(x => x._2 == "A" || x._2 == "B" || x._2 =="C")
val res = refgradeRDD.filter(x => x._2 != x._4).count
