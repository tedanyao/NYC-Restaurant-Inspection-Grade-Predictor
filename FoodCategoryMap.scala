val foodSet = Set(

)
var myFoodCategory = scala.collection.mutable.Map.empty[String, (Int, Int)]
var count = 0
var total = foodSet.toSeq.length
for (i <- foodSet) {
    myFoodCategory(i) = (count, total)
    count += 1
}

def foodToFeature(str: String): Array[Double] = {
    val count = myFoodCategory.getOrElse(str, (0,0))._1
    val total = myFoodCategory.getOrElse(str, (0,0))._2
    var v = Array.fill(total)(0.0)
    if (total != 0) {
        v.update(count, 1)
        v
    } else {
        Array(-1)
    }
}
