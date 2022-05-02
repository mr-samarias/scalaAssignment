val file = spark.read.format("csv").option("header","true").load("flightData.csv")
val splitdate = file.withColumn("year", year(col("date"))).withColumn("month", month(col("date"))).withColumn("day", dayofmonth(col("date")))
val columns = splitdate.select("month","flightId")
val rmdup = columns.dropDuplicates("flightId")
val sortmonths = rmdup.sort($"month".asc)
val countflights = sortmonths.groupBy("month").count()
val colsrename = countflights.withColumnRenamed("count","Number of Flights").withColumnRenamed("month", "Month")
val FperM = colsrename.sort($"Month".asc)
FperM.show
