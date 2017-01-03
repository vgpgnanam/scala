# scala
#Spark-shell


spark-submit --master yarn --conf "spark.ui.port=10101" test.py


dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
for i in dataRDD.take(10): print(i)

dataRDD.saveAsTextFile("/user/gnanaprakasam/scala/departmentTesting")

spark-submit --master local saveFile.py

spark-submit --master yarn saveFile.py

# save as sequencefile

dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
dataRDD.map(lambda rec: (None, rec)).saveAsSequenceFile("/user/gnanaprakasam/pyspark/departmentSeq")
dataRDDSeq = dataRDD.map(lambda rec: (None, rec))
for i in dataRDDSeq.take(10): print(i)


dataRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/departments")
dataRDD.map(lambda rec: tuple(rec.split("|", 1))).saveAsSequenceFile("/user/gnanaprakasam/pyspark/departmentSeqKey")

# saveasNewAPIHadoopFile

path="/user/gnanaprakasam/pyspark/departmentSeqNewAPI"
dataRDD.map(lambda x: tuple(x.split("|", 1))).saveAsNewAPIHadoopFile(
path,"org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")

# reading sequenceFile
dataRDD = sc.sequenceFile("/user/gnanaprakasam/pyspark/departmentSeq")
for i in dataRDD.take(10): print(i)

dataRDD = sc.sequenceFile("/user/gnanaprakasam/pyspark/departmentSeqNewAPI","org.apache.hadoop.io.IntWritable","org.apache.hadoop.io.Text")
for i in dataRDD.take(10): print(i)

# Code snippet to read data from hive tables in hive context.

from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
depts = sqlContext.sql("select * from departments")
for i in depts.take(10): print(i)







Filter
#Get all the orders with status COMPLETE

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val ordersCompleted = ordersRDD.filter(x => x.split(",")(3) equals("COMPLETE"))

ordersCompleted.take(10). foreach(println)

# Get all the orders where status contains the word PENDING

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val ordersPending = ordersRDD.filter(x => x.split(",")(3) contains("PENDING"))
ordersPending.take(10). foreach(println)

# Get all the orders where order_id is greater than 100

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val orderGT100 = ordersRDD.filter(x => (x.split(",")(0).toInt > 100))
orderGT100.take(10). foreach(println)

#Boolean operation - or
#Get all the orders where order_id > 100 or order_status is in one of the pending states

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val ordersGT100RPending = ordersRDD.filter(x => (x.split(",")(0).toInt > 100) || (x.split(",")(3) contains("PENDING")))
ordersGT100RPending.take(100). foreach(println)

# Get order id > 1000 and (Status PENDING or cancelled)

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val ordersGT100PenRCan = ordersRDD.filter(x => (x.split(",")(0).toInt > 100) &&
  ((x.split(",")(3) contains("PENDING")) || (x.split(",")(3) equals("CANCELED"))))
ordersGT100PenRCan.take(100). foreach(println)

# Get distinct order status from orders table
val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val orderstatus = ordersRDD.filter(x => x.split(",")(3) != "").distinct()
orderstatus.take(10).foreach(println)

# Orders > 1000 and staus other than COMPLETE

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val orderGT100Notcomplete = ordersRDD.filter(x => (x.split(",")(0).toInt > 1000 &&
  x.split(",")(3) != "COMPLETE"))
orderGT100Notcomplete.take(10). foreach(println)

# Check if there are cancelled orders with amount greater than 1000$

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/order_items")

val ordersCancelled = ordersRDD.filter(x => x.split(",")(3) equals("CANCELED"))
val ordersParsed = ordersCancelled.map(x => (x.split(",")(0).toInt, x))

val orderItemsParsed = orderItemsRDD.map(x => (x.split(",")(1).toInt, x.split(",")(4)))

val orderItemsJoinorders = orderItemsParsed.join(ordersParsed)

val revenuePerOrder = orderItemsJoinorders.map(x => (x._1.toInt, (x._2._1.split(",")(0).toFloat)))

val revenuePerOrderreduceByKey = revenuePerOrder.reduceByKey((x, y) => x + y)

val revenuePerOrderFilter = revenuePerOrderreduceByKey.filter(x => x._2 > 1000)

revenuePerOrderFilter.take(10). foreach(println)

import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)

sqlContext.sql("select * from " +
  "(select o.order_id, " +
    "sum(oi.order_item_subtotal) as order_item_revenue " +
    "from gnanaprakasam.orders o join gnanaprakasam.order_items oi " +
    "on o.order_id = oi.order_item_order_id " +
    "where o.order_status = 'CANCELED' " +
    "group by o.order_id) q " +
  "where order_item_revenue >= 1000")
.count()

# Sorting and Ranking using Global Key

val ordersRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/orders")
val ordersMap = ordersRDD.map(x => (x.split(",")(0).toInt, x)).sortByKey()
ordersMap.take(10).foreach(println)

ordersRDD.map(x => (x.split(",")(0).toInt, x)).sortByKey().take(5). foreach(println)
ordersRDD.map(x => (x.split(",")(0).toInt, x)).sortByKey(false).take(5).foreach(println)

ordersRDD.map(x => (x.split(",")(0).toInt, x)).takeOrdered(5).foreach(println)
ordersRDD.map(x => (x.split(",")(0).toInt, x)).takeOrdered(5) (Ordering[Int].reverse.on (x => x._1)).foreach(println)

ordersRDD.takeOrdered(5) (Ordering[Int].on (x => x.split(",")(0).toInt)).foreach(println)
ordersRDD.takeOrdered(5) (Ordering[Int].reverse.on(x => x.split(",")(0).toInt)).foreach(println)

# Sort products by price with in each category

productsRDD = sc.textFile("/user/gnanaprakasam/sqoop_import/products")
productsMap = productsRDD.map(x => (x.split(",")(1), x))
productsGroupBy = productsMap.groupByKey()
for i in productsGroupBy.map(x => (x._2.toList.sortBy(k => k.split(",")[4]).toFloat))).take(10): print(i)

for i in productsGroupBy.map(x => (x._2.toList.sortBy(k => -k.split(",")[4]).toFloat))).take(10): print(i)

for i in productsGroupBy.flatMap(x => (x._2.toList.sortBy(k => -k.split(",")[4]).toFloat))).take(10): print(i)


def getTopDenseN(rec (String, Iterable[String], topN: Int): Iterable[String] = {
  var topNPrices: List[Float] = List()
  var prodPrices: List[Float] = List()
  var sortedRecs: List[String] = List()
  for i <- rec._2 {
    prodPrices = prodPrices:+ i.split(",")[4].toFloat
  }
  topNPrices = prodPrices.distinct.sortBy(k => -k).take(topN)
  sortedRecs = rec._2.toList.sortBy(k => -k.split(",")[4].toFloat)
  var x: List[String] = List()
  for (i <- sortedRecs) {
    if (topNPrices.contains(i.split(",")(4).toFloat))
      x = x: + i
  	}
   return x
  }

  
products = sc.textFile("/user/gnanaprakasam/sqoop_import/products")
productsMap = products.map(rec => (rec.split(",")[1], rec))

for i in productsMap.groupByKey().flatMap(x => getTopDenseN(x, 2)).collect(): print(i)



