//https://kotlinlang.org/docs/scope-functions.html
fun main(args: Array<String>) {
    var a = 1
    var b = 2

    a = a.let { it + 2 }.let {
        val i = it + b
        i
    }
    println(a) //5

    //
    val numberList = mutableListOf<Double>()
    numberList.also { println("Populating the list") }
            .apply {
                add(2.71)
                add(3.14)
                add(1.0)
            }
            .also { println("Sorting the list") }
            .sort()
    println(numberList)


    //
    val numbers = mutableListOf("one", "two", "three")
    val countEndsWithE = numbers.run {
        add("four")
        add("five")
        count { it.endsWith("e") }
    }
    println("There are $countEndsWithE elements that end with e.")

    val numbers1 = mutableListOf("one", "two", "three", "four", "five")
    val resultList = numbers1.map { it.length }.filter { it > 3 }
    println(resultList)

}
