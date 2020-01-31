![Common Crawl Logo](https://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Ranking de responsabilidad ambiental empresarial

Este proyecto busca hacer conteo y evaluación de sentimiento de sitios en Internet, para 4 empresas colombianas.


## Configuración

Para desarrollar e instalarlo localmente, se requiere:
* Linux, sea máquina física, virtual o por [Windows Subsystem for Linux] (https://docs.microsoft.com/en-us/windows/wsl/about)
* Spark, puede instalarlo siguiendo las [instrucciones](https://spark.apache.org/docs/latest/). Tenga en cuenta que debe utilizar Java 8.
* Python 3
* Todos los módulos de Python, que se pueden instalar con el comando
```
pip3 install -r requirements.txt
```
* Descargar los archivos WARC a procesar, con las utilerías cdx-index-client.py y cdx-index-retrieval.py, siguiendo las [instrucciones](https://liyanxu.blog/2019/01/19/retrieve-archived-pages-using-commoncrawl-index/), y ubicarlos en la carpeta ../cc-output/warc/ (o en la especificada en [get-data-index.sh] (./get-data-index.sh) )


## Compatiblidad

Probado en Ubuntu instalado con WSL, Windows 10 2004, Spark 2.4.4, Python 3.6.9


## Generar índice de datos a procesar

Para generar los índices, se debe ejecutar el archivo [get-data-index.sh] (./get-data-index.sh), que generará en la carpeta input el listado de archivos a procesar, por subcarpeta detectada.


### Ejecutar localmente el conteo de palabras

Primero, apunte la variable de entorno `SPARK_HOME` a su instalación de Spark. 
Luego, envíe un trabajo con el comando

```
$SPARK_HOME/bin/spark-submit ./rrae_count.py \
	--num_output_partitions 1 --log_level WARN \
	./input/warc_all.txt rrae_count_all
```

Esto contará las palabras claves configuradas en [palabrasclave.py] (./palabrasclave.py) para las cuatro empresas, y almacenará los resultados en la tabla de SparkSQL "rrae_count_all" en la ubicación del warehouse de Spark definida por `spark.sql.warehouse.dir` (usualmente en su directorio de trabajo como as `./spark-warehouse/rrae_count_all`). T3nga en cuenta que el parámetro --num_output_partitions debería ser al menos el número de núcleos de su procesador.


El contenido puede ser accedido desde PySpark via SparkSQL, e.g.,

```
$SPARK_HOME/bin/pyspark
>>> df = sqlContext.read.parquet("spark-warehouse/servernames")
>>> for row in df.sort(df.cnt.desc()).take(10): print(row)
```

Y exportado a Excel 
```
$SPARK_HOME/bin/pyspark
>>> df = sqlContext.read.parquet("spark-warehouse/rrae_count_all")
>>> df.toPandas().to_excel('fileOutput.xls', sheet_name = 'Sheet1', index = False)
```

### Ejecutar localmente el análisis de sentimiento

Para el análisis de sentimiento en Azure, se debe tener una cuenta creada en Azure, y un nuevo servicio de tipo Text Analytics. Se puede utilizar la instancia Free, que permite 5000 transacciones por mes. Es [necesario crear un recurso para Text Analytics] (https://docs.microsoft.com/en-us/azure/cognitive-services/cognitive-services-apis-create-account) y modificar [rrae_score.py] (./rrae_score.py), para configurar el subscription key y el endpoint.

Para el análisis de sentimiento en Amazon, se debe tener una cuenta en Amazon Web Services y un usuario IAM. Luego se debe [configurar la interface de línea de comandos de AWS] (https://docs.aws.amazon.com/comprehend/latest/dg/setup-awscli.html) con el usuario y contraseña creado.

Luego, envíe un trabajo con el comando

```
$SPARK_HOME/bin/spark-submit ./rrae_score.py \
	--num_output_partitions 1 --log_level WARN \
	./input/warc_all.txt rrae_score_all
```


## Créditos

El código base está basado en el código provisto por CommonCrawl en su [repositorio] (https://github.com/commoncrawl/cc-pyspark); en especial el ejemplo de [contar nombres de servidores web] (https://github.com/commoncrawl/cc-pyspark/blob/master/server_count.py), y la [configuración del job de spark] (./sparkcc.py)

## Licencia

Licencia MIT, definida [aquí](./LICENSE)
