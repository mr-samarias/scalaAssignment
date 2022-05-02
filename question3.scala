val file2 = spark.read.format("csv").option("header","true").load("flightData.csv")
val colselect = filew.select("passengerId","from","to")
val nouk = colselect.filter(not($"from".contains("uk"))).filter(not($"to".contains("uk")))
val countid = nouk.groupBy("passengerId").count()
val sortid = countid.orderBy($"count".desc)
val colrenam = sortid.withColumnRenamed("passengerId","Passenger ID").withColumnRenamed("count","Longest Run")
colrenam.show
