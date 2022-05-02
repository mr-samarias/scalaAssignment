val file1 = spark.read.format("csv").option("header","true").load("flightData.csv")
val file2 = spark.read.format("csv").option("header","true").load("passengers.csv")
val sort1 = file1.orderBy($"passengerId".desc)
val sort2 = file2.orderBy($"passengerId".desc)
val drop1 = sort1.dropDuplicates("passengerId")
val sort3 = drop1.orderBy($"passengerId".desc)
val splfly = sort3.select("flightId")
val join1 = sort2.withColumn("id", monotonically_increasing_id)
val join2 = splfly.withColumn("id", monotonically_increasing_id)
val joinall = join2.join(join1, Seq("id")).drop("id")
val colrename = joinall.withColumnRenamed("flightId","Number of Flights").withColumnRenamed("passengerId","Passenger ID").withColumnRenamed("firstName","First name").withColumnRenamed("lastName","Last name")
val freq = colrename.orderBy($"flightId".desc)
freq.show(100)
