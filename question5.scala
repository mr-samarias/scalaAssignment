val file4 = spark.read.format("csv").option("header","true").load("flightData.csv")
val select4 =  file4.select("passengerId","flightId","date")
val fromto = select4.as("col1").join(select4.as("col2"),$"col1.passengerId" < $"col2.passengerId" && $"col1.flightId" === $"col2.flightId" && $"col1.date" === $"col2.date", "inner" ).groupBy($"col1.passengerId", $"col2.passengerId").agg(count("*").as("flights"),min($"col1.date").as("from"),max($"col1.date").as("to")).where($"flights" >= 3)
val colnames = fromto.toDF("Passenger 1 ID","Passenger 2 ID","Number of flights together","From","To")
colnames.show
