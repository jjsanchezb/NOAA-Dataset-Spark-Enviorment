from pyspark import SparkConf,SparkContext
import os
conf =  (SparkConf().setMaster("local[2]").setAppName("My app").set("spark.executor.memory", "1g").set("spark.driver.memory","3g").set("spark.serializer","org.apache.spark.serializer.KryoSerializer"))
sc = SparkContext(conf= conf)

##Unidades semanticas en SPARK:
#RDD
#Particion
#item o Par Clave Valor

##Unidades semanticas en el Dataset
#Dataset    => Coleccion de sensores => Carpeta con 4960 archivos
#Sensor     => Conjunto de 10 archivos => 10 Archivos
#Archivo    => 1 anio de medidas por sensor => 1 Archivo
#Medidas    => Cada una tiene mes, dia, hora  => 8760 o 8784



#Numero de Sensores => 496
#Numero de Archivos => 4960     <= 10 por sensor 
#
#Numero de medidas ideal por archivo => 365*24 = 8760, Anio bisiesto => 366*24 = 8784 
#Numero de medidas ideal por sensor  => 8760*8 + 8784*2 = 87648 
#
#Numero maximo de medidas en total   => 496*87648 = 43.473.408 



# Algoritmo
# Del maximo de medidas  => 43.473.408 se conservan solo las que estan entre las 12 y 18 horas 12.679.744 ::::FILTRO POR HORAS  CACHE               | Mas Datos
#                                                                                                                                                   |
# Numero maximo de medidas en total   => 12.679.744                                                                                                 |
# Numero de medidas ideal por sensor  => 12.679.744 / 496 = 25.564                                                                                  |
#                                                                                                                                                   |
# De las medidas entre las 12 y 18 horas se eliminan las que tiene un ambiente "Confortable" -1 < medida < 17 ::: FILTRO POR MEDIDA                 | Menos Datos
# TENGO LAS MEDIDAS INCONFORTABLES ENTRE LAS 12 y 18 DE CADA DIA                                                                                    |    
#                                                                                                                                                   |    
# De los sensores se descartaran aquellos donde la mitad del tiempo entre las 12 y 18 horas                                                         |
#    durante 10 anios sea confortable => NUMERO DE MEDIDAS DEL SENSOR < 12.782                   :::FILTRO POR NUMERO DE MEDIDAS DEL SENSOR          | Datos Finales
#
# TENGO LOS SITIOS INCONFORTABLES DE ESTADO UNIDOS: AQUELLOS DONDE EN PROMEDIO POR 3 HORAS AL DIA SE SIENTE INCONFORTABLE


pathFilesAvailables = "/home/hduser/Desktop/Workspace/New1/filesAvailables.txt"
pathDataset = "file:///home/hduser/Desktop/Workspace/New1/Dataset"


##FUNCIONES PARA RDD DE UN LUGAR
def isAvailableHourRecord(line):
    hora = int( line[11:14] )
    dewpoint = int( line[20:26] ) 
    if ( hora >= 12 and hora <= 18 )  and dewpoint != -9999: 
        return True
    else:
        return False 

def isAvailableRecord(dewpoint):

    if (dewpoint > -10 and dewpoint < 170):
        return False
    else:
        return True 

def onliDewPoints(line):
    dewpoint = int( line[20:26] ) 
    return dewpoint

def isOver(dewpoint):
    return not(dewpoint < 170)

def isUnder(dewpoint):
    return not(dewpoint > -10)

def suma(a,b):
    return a + b


##OBTENIENER LA LISTA DE ARCHIVOS JUNTOS POR ESTACION  
f = open("filesAvailables.txt")
files = []
for i in f:
    if (i != ""):
        files.append(i)
f.close()

files = sorted(files) #Contiene la lista completa de archivos


##AGRUPA LOS NOMBRE DE ARCHIVOS POR ESTACION
filesByPlace = [] #Contiene los 496 lugares
place = [] #Contiene los 10 archivos por lugar
count = 0
for i in files:
    count += 1
    place.append(i)
    if count == 10:
        filesByPlace.append(place)
        place = []
        count = 0

##PASAR LOS NOMBRES DE ARCHIVO A RDDs
rddsOnePlace =  []
listPlaces = []
for place in filesByPlace:
    rddsOnePlace =  []
    count = 0
    for f in place:
        f = f[:-1]
        pathFile = pathDataset + os.sep + f 
        rdd = sc.textFile(pathFile)   
        rddsOnePlace.append(rdd)
    listPlaces.append([rddsOnePlace,f[:-5]])

for rddsPlace in  listPlaces:
    #ARMANDO EL RDD PARA EL SENSOR rddSensor
    print "Nuevo Sensor"
    sensorName = rddsPlace[1]
    print "Nombre de SENSOR:    " + sensorName
    rddSensor =  sc.union(rddsPlace[0])

    #APLICANDO ALGORITMO AL LUGAR
    rddSensorDewPointHourAvailable =  rddSensor.filter(isAvailableHourRecord).map(onliDewPoints).cache()

    numRecordsSensorAvailables = rddSensorDewPointHourAvailable.filter(isAvailableRecord).count()
    numRecors = rddSensorDewPointHourAvailable.count()
    isConfort = numRecordsSensorAvailables < 12782
    
    print "Numero de medidas inconfortables del sensor:\t\t\t" + str(numRecordsSensorAvailables) + "\t\t\tNumero de medidas:\t\t\t" + str(numRecors) +"\t\t\tLugar Confortable\t\t\t" + str( isConfort )
    if ( isConfort ):
        continue

    
    
    recordsOverF = rddSensorDewPointHourAvailable.filter(isOver)
    numRecordsOverF = recordsOverF.count()
    if ( numRecordsOverF != 0):
        sumRecordsOver = recordsOverF.reduce(suma)     
    else:
        sumRecordsOver = 0

        

    recordsUnderF= rddSensorDewPointHourAvailable.filter(isUnder)
    numRecordsUnderF = recordsUnderF.count()
    if (numRecordsUnderF != 0):
        sumRecordsUnder = recordsUnderF.reduce(suma)
    else:
        sumRecordsUnder = 0

      
    
    w = open("resultsPromedio","a")
    w.write(sensorName + "\t\t\tNumero de medidas:\t\t\t" + str(numRecors) + "\t\t\tNumero de medidas inconfortables del sensor:\t\t\t" + str(numRecordsSensorAvailables) +"\t\t\tDew Points sobre 17:\t\t\t" +  str(numRecordsOverF) + "\t\t\tSuma Dew Pints sobre 17:\t\t\t" + str(sumRecordsOver)  + "\t\t\tDew Points bajo -1:\t\t\t" + str(numRecordsUnderF) + "\t\t\tSuma Dew Pints bajo -1:\t\t\t" + str(sumRecordsUnder)  + "\n")
    w.close()


#isd-history = sc.textFile("file:///home/hduser/Desktop/Workspace/SourceDocuments/isd-history.txt")


#CODIGOS USADOS PARA PRUEBAS

##PROBANDO EL ORDEN DE LA LISTA DE ARCHIVOS JUNTOS POR ESTACION
# for n in files:
#     print n

##PROBANDO FILESBYPLACE
# for n in filesByPlace:
#     print n


#PROBANDO EL ARREGLO LISTPLACES
# print "tipo de listPlaces  " + str( type(listPlaces) ) 
# print len(listPlaces)
# for lines in listPlaces:
#     print "tipo de rddsOnePlace  " +  str( type(lines) )
#     print len(lines)
#     for line in lines:
#         print "tipo de rdd  " + str( type(line) )

    
    
