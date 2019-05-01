// input: mergeRDD

mergeRDD.persist
val m2 = mergeRDD.map(line => Array(
    line(0).toString.toDouble,
    priceToInt(line(1).toString).toDouble,
    line(2).toString.toDouble,
    line(3).toString.toDouble,
    gradeToInt(line(4).toString).toDouble)
)

val m3 = m2.map(line => line(4).toString + " " + "1:" + line(0) + " 2:" + line(1) + " 3:" + line(2))
m3.saveAsTextFile("hdfs:/user/yyl346/project/a.log")
