//path of files
val linkage = "C:/spark-scala/adv_analyt_spark/linkage/block*"
//define first rdd
val rawblocks = sc.textFile(linkage)
rawblocks.first
//
val head = rawblocks.take(10)
head.foreach(println)
def isHeader(line: String) = line.contains("id_1")
head.filter(isHeader).foreach(println)
head.filterNot(isHeader).length
head.filterNot(isHeader).foreach(println)
head.filter(x=> !isHeader(x)).length
head.filter(!isHeader(_)).length
val noheader = rawblocks.filter(x => !isHeader(x))
val noheader = rawblocks.filter(!isHeader(_))
noheader.first
val prev = spark.read.csv(linkage)

val parsed = spark.read.
  option("header", "true").
  option("nullValue", "?").
  option("inferSchema", "true").
  csv(linkage)
  
parsed.printSchema()

//don't run
val d1 = spark.read.format("json").load("file.json")
val d2 = spark.read.json("file.json")
|
d1.write.format("parquet").save("file.parquet")
d1.write.parquet("file.parquet")
// to ignore overwrite errors
d2.write.mode(SaveMode.Ignore).parquet("file.parquet")

parsed.count()
parsed.cache()
// RDD method
parsed.rdd.
  map(_.getAs[Boolean]("is_match")).
  countByValue()
// DF method
parsed.
  groupBy("is_match").
  count().
  orderBy($"count".desc).
  show()

// sums, mins, maxes, means, and standard deviation 
// using the agg method of the DataFrame API in 
// conjunction with the aggregation functions defined 
// in the org.apache.spark.sql.functions package.
 parsed.agg(avg($"cmp_sex") as "avg_sex", stddev($"cmp_sex") as "stddev_sex").show()
 
 parsed.createOrReplaceTempView("linkage")
 
 spark.sql("""  SELECT is_match, COUNT(*) cnt 
				FROM linkage  
				GROUP BY is_match  
				ORDER BY cnt DESC
				""").show()
				
val summary = parsed.describe()
summary.show()

val matches = parsed.where("is_match = true")
val matchSummary = matches.describe()

val misses = parsed.filter($"is_match" === false)
val missSummary = misses.describe()

summary.printSchema()

val schema = summary.schema
val longForm = summary.flatMap(row => {
  val metric = row.getString(0)
  (1 until row.size).map(i => {
    (metric, schema(i).name, row.getString(i).toDouble)
  })
})

val longDF = longForm.toDF("metric", "field", "value")
longDF.show()

//Reshape Longform DF
// groupby field and then pivot out field
// and grab the first value
val wideDF = longDF.
  groupBy("field").
  pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
  agg(first("value"))
wideDF.select("field", "count", "mean").show()
