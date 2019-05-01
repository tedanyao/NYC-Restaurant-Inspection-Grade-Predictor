val foodSet = Set(
"African", // 7
"Tex-Mex",
"Latin (Cuban, Dominican, Puerto Rican, South & Central American)",
"Donuts",
"American",
"Chinese",
"Spanish",
"Bakery",
"Mexican",
"Japanese",
"Italian",
"Chicken",
"Mediterranean",
"Irish",
"Steak",
"Indian",
"Soul Food",
"Asian",
"Polish",
"Barbecue",
"Caribbean",
"Delicatessen",
"Hawaiian",
"Greek",
"Pizza/Italian",
"CafÃ©/Coffee/Tea",
"Vietnamese/Cambodian/Malaysia",
"Bangladeshi",
"French",
"Sandwiches/Salads/Mixed Buffet",
"Vegetarian",
"Pizza",
"Hamburgers",
"Turkish",
"Jewish/Kosher",
"Korean",
"Portuguese",
"Thai",
"Other",
"Bagels/Pretzels",
"Chinese/Japanese",
"Juice, Smoothies, Fruit Salads",
"German",
"Middle Eastern",
"Eastern European",
"Seafood",
"Bottled beverages, including water, sodas, juices, etc.",
"Pakistani",
"Sandwiches",
"Creole",
"Salads",
"Australian",
"Peruvian",
"Tapas",
"Ice Cream, Gelato, Yogurt, Ices",
"Hotdogs",
"Pancakes/Waffles",
"Ethiopian",
"Brazilian",
"Soups & Sandwiches",
"Russian",
"English",
"Chinese/Cuban",
"Soups",
"Cajun",
"Egyptian",
"Czech",
"Hotdogs/Pretzels",
"Armenian",
"Continental",
"Fruits/Vegetables",
"Moroccan",
"Indonesian",
"Filipino",
"Not Listed/Not Applicable",
"Creole/Cajun",
"Iranian",
"Californian",
"Scandinavian",
"Nuts/Confectionary",
"Southwestern",
"Afghan",
"Basque",
"Chilean"
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
