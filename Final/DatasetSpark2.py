from pyspark import SparkConf,SparkContext

conf =  (SparkConf().setMaster("local[2]").setAppName("My app").set("spark.executor.memory", "1g").set("spark.driver.memory","3g").set("spark.serializer","org.apache.spark.serializer.KryoSerializer"))
sc = SparkContext(conf= conf)


def filter1(item):
    country = item[43:45]
    if (country == "US"):
        return True
    

def maper1(item):
    usaf = item[0:6]
    wban = item[7:12]
    lat =  item[57:64]
    log =  item[65:73]
    nombre = item[14:43]
    return usaf + "-" + wban, nombre + "," + str(lat) + "," + str(log)



w = open("resultsPromedio")
vector = []
for i in w:
    data = i.split("			",12)
    id_sensor = data[0]
    num_medidas =int(data[2]) 
    num_medidas_inconfortables = int(data[4])
    num_Dew_Points_Over = int(data[6])
    sum_Dew_Points_Over = int(data[8])
    num_Dew_Points_Under = int(data[10])
    sum_Dew_Points_Under = int(data[12])
    if (num_Dew_Points_Over != 0):
        promOver = sum_Dew_Points_Over/num_Dew_Points_Over
    else:
        promOver = 0
    if (num_Dew_Points_Under != 0):
        promUnder = sum_Dew_Points_Under/num_Dew_Points_Under
    else:
        promUnder = 0

    vector.append( (id_sensor,str( data[1] + "         " + str(num_medidas)+ "         " + data[3] + "         " + str(num_medidas_inconfortables)+ "         " + data[5]+ "         " + str(num_Dew_Points_Over)+ "         " + "Promedio medidas sobre 17"+ "         " +  str(promOver)+ "         " + data[9]+ "         " + str(num_Dew_Points_Under)+ "         "  + "Promedio medidas bajo -1"+ "         " + str(promUnder)  ) )  )

w.close()

pathIsdHistory = "file:///home/hduser/Documents/Workspace/SourceDocuments/isd-history.txt"
rddCardi = sc.textFile(pathIsdHistory).filter(filter1).map(maper1)
rddData = sc.parallelize(vector)

def maper3(joinData):
    # usaf_wban,measures,lat_long = joinData
    return joinData[1][1]

res = rddData.join(rddCardi).map(maper3).collect()

r = open("results.txt","w")
for item in res:
    r.write( str(item) + "\n" )
r.close()


