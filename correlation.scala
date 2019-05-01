// covariance matrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vector

def mytoString(s: String) :Double = {
    var d = 0.0
    for (char <- s) {
        if ('0' <= char && char <= '9')
            d = d*37 + (char - '0')
        else if ( 'a' <= char && char <= 'z')
            d = d*37 + (char - 'a')
        else if ( 'A' <= char && char <= 'Z')
            d = d*37 + (char - 'A')
        else
            d = d* 37 + (char - 20)
    }
    d
}



val mergeRDD = MERGE_DF.select("rating", "price", "review_count", "SCORE", "GRADE").rdd
mergeRDD.persist
val testRDD = mergeRDD.map(row => {
  Vectors.dense(row.toSeq.toArray.map({
    case d: Double => d
    case s: String => mytoString(s)
    case i: Integer => i.toDouble
    case l: Long => l.toDouble
    case _ => 0.0
  }))
})
val correlMatrix: Matrix = Statistics.corr(testRDD, "pearson")
val Vrdd = sc.parallelize(correlMatrix.toArray.toSeq)
Vrdd.take(4).foreach(println)
