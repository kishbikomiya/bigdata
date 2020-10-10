# This code is written by one of our student. Works only in Spark2.0 and above

case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

def parseLogLine(log: String):
	LogRecord = {
		val res = PATTERN.findFirstMatchIn(log) 
		if (res.isEmpty)
		{
			println("Rejected Log Line: " + log)
			LogRecord("Empty", "", "",  -1 )
		}
		else 
		{
			val m = res.get
			LogRecord(m.group(1), m.group(4),m.group(6), m.group(8).toInt)
		}
		def containsURL_2(line:String):Boolean = return line matches "^.+(\"GET.+HTTP.*\").+$"
//the () is the part that will be extracted
def extractURL_2(line:String):(String) = {
    // Here we are using the regular expression for matching the strings with a certain pattern.
    val pattern = "^.+(\"GET.+HTTP.*\").+$".r
    val pattern(ip:String) = line
    return (ip.toString)
}
var URLaccesslogs = logFile.filter(containsURL_2)
URLaccesslogs.take(10)
var URLs = URLaccesslogs.map(line => (extractURL_2(line),1));
URLs.take(10)
var URLcounts = URLs.reduceByKey((a,b) => (a+b))
var URLcountsOrdered = URLcounts.sortBy(f => f._2, false);
URLcountsOrdered.take(10)
	}

val logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")
val accessLog = logFile.map(parseLogLine)
val accessDf = accessLog.toDF()
accessDf.printSchema
accessDf.createOrReplaceTempView("nasalog")
val output = spark.sql("select * from nasalog")
output.createOrReplaceTempView("nasa_log")
spark.sql("cache TABLE nasa_log")

spark.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show

spark.sql("select host,count(*) as req_cnt from nasa_log group by host order by req_cnt desc LIMIT 5").show

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show

spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show

spark.sql("select httpCode,count(*) as req_cnt from nasa_log group by httpCode ").show