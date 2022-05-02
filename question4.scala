val file3 = spark.read.format("csv").option("header","true").load("flightData.csv")
val select3 =  file3.select("passengerId","flightId")
val togpass = select3.as("sel1").join(select3.as("sel2"),$"sel1.passengerId" < $"sel2.passengerId" && $"sel1.flightId" === $"sel2.flightId" , "inner" ).groupBy($"sel1.passengerId", $"sel2.passengerId").agg(count("*").as("flightsTogether")).where($"flightsTogether" >= 3)
val colname = togpass.toDF("Passenger 1 ID","Passenger 2 ID","Number of flights together")
colname.show
