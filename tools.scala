def priceToInt(str: String): Double = {
    var s = 0
    if (str == "$")
        s = 1
    else if (str == "$$")
        s = 2
    else if (str == "$$$")
        s = 3
    else if (str == "$$$$")
        s = 4
    s
}

def scoreToInt(str: String): Double = {
    var s = 0.0
    if (str == "")
        s = 0.0
    else if (str == "null")
        s = 0.0
    else
        s = str.toDouble
    s
}

def gradeToInt(grade: String): Double = {
    var s = 0
    if (grade == "A")
        s = 1
    else if (grade == "B")
        s = 2
    else if (grade == "C")
        s = 3
    else if (grade == "P")
        s = 4
    s
}

def dateToInt(date: String): Double = {
    var arr = date.split("/")
    var month = arr(0)
    var day = arr(1)
    var year = arr(2)
    (year.toString + month.toString + day.toString).toInt
}

def scoreToGrade(score: Double): String = {
    if (score < 13.5) {
        "A"
    } else if (score < 27.5) {
        "B"
    } else {
        "C"
    }
}
def printFeature(feature: (Any, Array[Double])): String = {
    val label = feature._1
    var s = label.toString
    var index = 0
    for (i <- feature._2) {
        index += 1
        s += (" " + index + ":" + i.toString)
    }
    s
}

def mapTo01(arr: Array[Double], maxVal: Array[Double], minVal: Array[Double]): Array[Double] = {
    for (i <- 0 to arr.length - 1) {
        if (maxVal(i) == minVal(i)) {
            arr(i) = arr(i)
        } else {
            arr(i) = (arr(i) - minVal(i)) / (maxVal(i) - minVal(i))
        }

    }
    arr
}
