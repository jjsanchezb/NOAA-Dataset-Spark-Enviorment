from pyspark import SparkConf,SparkContext
import os

conf = SparkConf()
sc = SparkContext(conf= conf)
print ("Spark UI Web:    " + sc.uiWebUrl)


#PARTE 1 OBTENER LOS ODENTIFICADAORES DE LOS SENSORES A REVISAR

#direccion del archivo isd-history.txt
pathIsdHistory = "file:///home/hduser/Desktop/Workspace/SourceDocuments/isd-history.txt"



rddLines = sc.textFile(pathIsdHistory)
numPartitions = rddLines.getNumPartitions()
print ("Numero de particiones para isd-history: " + str(numPartitions))



#Elege los sensores en estados unidos con almenos 10 anios de datos 
def isAvailable1(line):
    country = line[43:45]
    timeBegin = line[82:86]
    endTime = line[91:95]
    if (country == "US" and timeBegin < "2000" and int(endTime) >= 2010):
        return True

#Devulve el codigo usaf-wban que corresponde a un identificador de sensor 
#El identificador esta presente en el nombre de los archivos con los datos del sensor 
def getInfo1(line):
    usaf = line[0:6]
    wban = line[7:12]
    return usaf + "-" + wban

rddStationsAvailable = rddLines.filter(isAvailable1).map(getInfo1).cache()
listStationsAvailable = rddStationsAvailable.collect()



broadcastStationAvailable = sc.broadcast(listStationsAvailable)

print ("OK parte 1")
print ("numero de estaciones a revisar:  " + str(len(listStationsAvailable)))


#PARTE 2 OBTENER LOS NOMBRES DE LOS ARCHIVOS A REVISAR



#direccion del dataset
pathDataset = '/media/hduser/C0143B37143B3030/Users/Jhon Jairo Sanchez B/Documents/SharedDebian/JJ/isdlite-normals-003/isdlite'

filesDataset = os.listdir(pathDataset)
rddFilesDataset = sc.parallelize(filesDataset)
numPartitions = rddFilesDataset.getNumPartitions()
print ("numero de particiones para nombres de archivos en el dataset:   " + str(numPartitions))

#Elige los archivos de los sensores en EU que contienen medidas entre 2001 y 2010
def isAvailable2(line):
    idStation = line[0:12]
    yearStation = line[13:17]
    if ( (idStation in broadcastStationAvailable.value) and int(yearStation) > 2000 and (int(yearStation) <= 2010) ):  
        return True


rddFilesAvailables = rddFilesDataset.filter(isAvailable2).map(lambda x: (x[0:12],x) ).cache()


keyValueFA = rddFilesAvailables.countByKey()
rddListKeyValueFA = sc.parallelize(keyValueFA.iteritems()).filter(lambda x: x[1]==10)
listKeyValueFA = rddListKeyValueFA.map(lambda x: x[0]).collect()
broadcastFilesAvailables = sc.broadcast(listKeyValueFA)

#lista de los 10 archivos por sensor con medidas entre 2001 y 2010 
filesAvailables = rddFilesAvailables.filter(lambda x: x[0] in broadcastFilesAvailables.value).map(lambda x: x[1]).collect()

#PARTE 3 OBTENER la cantidad de informacion de los archivos a analisar
finalSize = 0
for fi  in filesAvailables:
    arch = pathDataset + os.sep + fi
    est = os.stat(arch)
    tam = est.st_size
    finalSize += tam 
print "Cantidad de informacion a analisar:  " + str(finalSize)

#PARTE 4 guardar los nombres de los archivos a analizar
f = open("filesAvailables.txt","w")
for i in filesAvailables:
    f.write( str(i) + "\n")
f.close()


#PARTE DE PRUEBA

# for item,count in listKeyValueFA.iteritems():
#     print (item + " " + str(count))
# for item in filesAvailables:
#     print item



# Codigo Viejo
# filesDataset = []
# for station in listStationsAvailable:
#     print (station)
#     patron = pathDataset + "/" + station + "-"  + "20" + "[0-1]" + "[0-9]"
#     filesStation = glob.glob(patron)
#     filesDataset.append( filesStation )

# f = open("filesToRead.txt","w")
# for file in filesDataset:
#     f.write( str(file) + "\n")
# f.close()