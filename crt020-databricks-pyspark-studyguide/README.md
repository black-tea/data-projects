# CRT020: Apache Spark 2.4 w/ Python 3

Each of the sections includes a reference for further reading in the Spark Documentation and/or _Spark: The Definitive Guide_, which I will refer to as "SDG" with the appropriate chapter/section. Although this study guide is specifically focused on the Python API, much of it applies to the other language APIs as well.

Because you are allowed to referencing the Spark API documentation, you should get familiar looking up 

## Spark Architecture Components

* Driver: Translates user/program input into work and orchestrates this work across executors. It maintains the state of the Spark application and interfaces with the cluster manager in order to get physical resources.
* Executor: Executors carry out to work assigned by the driver and report the state of computation back to the driver node.
* Cluster Manager: Responsible for maintaining a cluster of machines to execute the Spark Application. Also has driver and worker node abstraction, but these are tied to physical machines instead of processes. Sparks supports three cluster managers: Spark's native cluster manager, Hadoop YARN, Apache Mesos.
* Cores/Slots/Threads:
* Partitions: Represent the physical distribution of data. Each partition is a collection of rows that sit on one physical machine and should have its own separate executor. 
* Catalog: The catalog is a repository of all table and DataFrame information, used to resolve columns and tables in the analyzer.
  
_Source: SDG (Ch 15 - "Architecture of a Spark Application")_

## Spark Execution

_**Execution Modes**_
* Cluster mode: Cluster manager launches driver process on worker node, along with executor processes.
* Client mode: Similar to cluster mode, but driver process remains on client machine
* Local mode: Entire Spark application is run on a single machine.
  
_Source: SDG (Ch 15 - "Execution Modes")_

_**Life Cycle of Spark Application**_
1. Client Request: Client submits pre-comiled JAR. Client machine requests from the cluster manager a Spark driver process. Cluster manager places a driver node onto a cluster machine.
2. Launch: Driver process begins running user code. Using `SparkSession`, requests executors from cluster manager. Cluster manager launches executor process to create a full "Spark Cluster".
3. Execution: Spark driver assigns tasks to executors, which respond with success/failure status updates.
4. Completion: When the application completes, the cluster manager process returns success/failure status and shuts down the executors.
  
_Source: SDG (Ch 15 - "Life Cycle of a Spark Application, Outside Spark")_

_**Spark Application: Structured API Execution**_
1. User code creates `SparkSession` instance, which instantiates the `SparkContext` and `SQLContext`. The `SparkContext` object represents the connection to the Spark Cluster.
2. User writes DataFrame/Dataset/SQL Code
3. Spark checks validity using the catalog. If valid, converts to an optimized _Logical Plan_ by pushing down predicates or selections.  
4. With the optimized _Logical Plan_, Spark creates several _Physical Plans_, compares each through a cost model, and then selects the best one.  
5. Spark executes the best _Physical Plan_ across the cluster.
  
_Source: SDG (Ch 15 - "Life Cycle of a Spark Application, Inside Spark")_

_**Spark Application: Components**_  
* Application: Each Spark application is made up of one or more _Spark Jobs_.
* Job: There will be one Spark Job per action, and each Job includes a set of stages, the number of which depends on how many shuffles need to take place.
* Stages: Stages represent a set of tasks to compute the same operation across machines in the cluster. A new stage begins for each tranformation that requires a shuffle.
* Tasks: Each task corresponds to a combination of a data partition and a set of transformations that run on a single executor.      
  
_Source: SDG (Ch 15 - "A Spark Job", "Stage", "Tasks")_

## Spark Concepts

* High-level Cluster Configuration  

### Distributed Shared Variables
* Broadcasting: In cases where one of the DataFrames in a join is sufficiently small, a broadcast join duplicates that DataFrame on every machine, avoiding the cost of shuffling the bigger DataFrame.
* Accumulators:  

_Source: [Spark Docs](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables), SDG (Ch 14 - "Broadcast Variables", "Accumulators")_

### DataFrame Operations: Transformations vs. Actions
* Transformations: A transformation is a modification to an immuatable data structure. Transformations have _lazy evaluation_, meaning that they won't be evaluated until an _action_ is performed on the data. This lazy evaulation allows Spark to assess all the transformations and assemble an efficient physical execution plan.
* Actions: Actions trigger the computations defined in the tranformation plan. There are 3 types of actions:
    * Actions to view data in the console
    * Actions to collect data to native objects in the respective language
    * Actions to write to output data sources  

_Source: [Spark Docs](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations)_ 

#### Wide vs. Narrow Transformations
* Narrow Transformations: Transformations where each input partition will contribute to only one output partition. With narrow partitions, Spark can employ _pipelining_, where everything is performed in-memory (and is therefore quite fast). Narrow transformations include map and filter functions.
  * Pipelining: Spark tries to perform as many computations as possible before writing data to memory or disk. With pipelining, a sequence of transformations that doesn't require a shuffle is collapsed into a single stage of tasks.
* Wide Tranformations: Transformations where input partitions will contribute to many output partitions, performing what is called a _shuffle_. When we perform a shuffle, Spark writes the results to disk, which slows down the execution time considerably. Wide transformations include non-broadcasted joins, aggregations, repartitions, window functions.
  * Shuffling: For all shuffle operations, Spark automatically caches to disk so it can be reused for multiple jobs and so the engine can re-run reduce tasks on failure without re-running all the input tasks.
  
_Source: SDG (Ch 15.  - "Pipelining", "Shuffle Persistence")_  

### RDD Persistence  
Each persisted RDD can be stored using a different storage level, the main levels listed in the table below. The default storage level is `StorageLevel.MEMORY_ONLY`, which should be used in almost all cases. 

Storage Level | Meaning | When to use 
--- | --- | ---
MEMORY_ONLY | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. Default. | Use in most cases.
MEMORY_ONLY_SER | Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read. | Try next if RDDs do not fit using MEMORY_ONLY option.
MEMORY_AND_DISK | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, spill to disk, and read them from there when they're needed. | Use only if functions that computed your datasets are expensive or filter out a large amount of data.
DISK_ONLY | Store the RDD partitions only on disk. | Use sparingly, since recomputing a partition may be as fast as reading from disk.  

_Source: [Spark Docs](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)_

## SparkContext

We can use `SparkContext` to control basic configuration settings:  
```python
spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.set("spark.executor.memory", "2g")
```

## Creating / Importing a DataFrame
In order to operate on a DataFrame, we first need to create or import one. 

#### Creating a DataFrame
The createDataFrame method takes in the following args:
_createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)_ where `data` is an RDD, python list, or `pandas.DataFrame`
  
```python
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Create df from list w/ list of fieldnames as schema
df = spark.createDataFrame([('Suzy', 22)], ['Name', 'Age']).show()

# Create df from list using defined schema
my_schema = StructType([
  StructField('Name', StringType(), True),
  StructField('Age', IntegerType(), True)
  ])

df = spark.createDataframe([('Suzy', 22)], schema=my_schema).show()

# Create df from python dict
df = spark.createDataFrame([{'Name': 'Suzy', 'Age': 22}]).show()

# Create df from range of numbers
range_df = spark.range(1000).toDF("number").show()
```
_Source: SDG (Chapter 5. Basic Structured Operations - "Creating DataFrames")_

#### DataFrameReader
 
We use the `DataFrameReader` class to read data from a souce and convert it into a DataFrame. We can read in data from the "core" formats (CSV, JSON, JDBC, ORC, Parquet, text, and tables) or other formats using the `format` and `load` methods.
```python
my_file = '/data/mydata.csv'

# CSV (core format) syntax
df = spark.read.csv(my_file, header=True, inferSchema=True)
display(df) 

# Can read in any format using below syntax
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(my_file)

df.show()
```

_Source: [pyspark.sql.DataFrameReader](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader), SDG (Ch 9. Data Sources)_

#### Schemas 
A schema defines the column names and types of a DataFrame. We can either infer the schema automatically (_schema on read_) or define it manually before reading in the data. Schema-on-read can cause issues related to data precision types, won't catch as many errors in the data, and is an expensive operation. For production, it is a good idea to manually define your schemas, especially for untyped inputs like CSV and JSON and for when you only want to parse a subset of the fields.

We can examine the Schema of an object:
```python
df.printSchema()
```
  
A schema is a `StructType` made up of `StructField`s. A `StructField` contains a name, type, and boolean indicating whether the column can have null or missing values.  The following types exist in Spark:  

Numeric | General | Time | Composite  
--- | --- | --- | ---  
`FloatType` | `StringType` | `TimestampType` | `ArrayType`  
`IntegerType` | `BooleanType` | `DateType` | `MapType`  
`DoubleType` | `NullType` ||   
`LongType` |||  
`ShortType` |||  
   
Creating a schema and enforcing it on read-in: 
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

manual_schema = StructType([
    StructField('COLUMN_NAME1', StringType(), False),
    StructField('COLUMN_NAME2', IntegerType(), True)
])

my_df = sparks.read.format('csv').schema(manual_schema).load('data/test.csv')
```  
_Source: SDG (Ch 9. Data Sources), SDG (Ch 5. Basic Structured Operations - "Schemas")_

#### DataFrameWriter
Just as reading data into DataFrames used the `pyspark.sql.DataFrameReader` class, we will write out using the `pyspark.sql.DataFrameWriter` class. As with reading in, can write data to the "core" formats or write data to other formats using format(), save(), with additional configuration options
```python
# Examples of "core" formats
df.write.parquet('myparquetfile')
df.write.saveAsTable('mytable')

# Can write to any format using below config
df.write.format('csv').mode('overwrite').option('sep', '\t').save('my-tsv-file.tsv')
```  
_Source: [pyspark.sql.DataFrameWriter](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter), SDG (Ch 9. Data Sources)_

## Working with DataFrames
Once you have a DataFrame, you are able to operate on it. These DataFrame _operations_ can be grouped into two categories: actions and transformations. For further detail on DataFrame methods, reference [pyspark.sql.DataFrame](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame) as well as SDG Ch 5: Basic Strutured Opeartions.  

### Common DataFrame Actions  
Collecting Rows to the driver: `.collect()` gets all the data from the entire DataFrame, `.take()` selects the first N rows, and `.show()` prints out a number of rows nicely. Be careful - a large collection can crash the driver!
```python
collectDF = df.limit(10) # to prevent collect from crashing driver
collectDF.take(5)
collectDF.show(5)
collectDF.collect()
```

### Common DataFrame Transformations

#### Distinct Rows / Duplicate Data
Use the `.distinct()` method on a DataFrame for unique rows. We can also drop rows with duplicate data. Within the method, specify the names of the columns that will be used together to identify duplicates.
```python
duplicateDF = spark.createDataFrame([
  (15342, "Conor", "red"),
  (15342, "conor", "red"),
  (12512, "Dorothy", "blue"),
  (5234, "Doug", "aqua")], 
  ["id", "name", "favorite_color"]
)

df = duplicateDF.distinct()
df = duplicateDF.dropDuplicates(['id', 'favorite_color']) # subset
df = duplicateDF.drop_duplicates(['id', 'favorite_color']) # same
```

#### Filtering Rows
We can either provide an expression as a string or build an expression using column manipulations. Both `.filter()` and `.where()` perform exactly the same. 
```python
from pyspark.sql.functions import col

df.filter(col('count') > 2).show(2)
df.where('count > 2').show(2) # str expression, same as above

# AND filter chaining
df.where('count > 2')\
  .where(col('ORIGIN_COUNTRY_NAME') != 'Croatia')\
  .show(2) 
```
   
#### Sorting Rows
We order rows using either of the two equivalent methods, `.sort()` and `.orderBy()` with the helper functions `asc()` and `desc()` to specify the direction of the order. Use `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, `desc_nulls_last()` for proper null handling.
```python
from pyspark.sql.function import asc, desc

# Order by count descending, and then 'myColumn' ascending
df.orderBy(col('count').desc(), col('myColumn').asc()).show(2)
df.sort(desc(col('count')), asc(col('myColumn'))).show(2) # same

# add example with proper null handling
``` 
  
#### Unions & Joins
Use the `.union()` DataFrame method to concatenate two DataFrames. FYI - Unions are based on location, not schema.
```python
# Union df1 & df2
newDF = df1.union(df2)

# Join

```

#### Split & Sample
Machine learning applications often require you to break up your DataFrame into train/test sets. You can easily perform this operation by supplying the proportion for the split. More generally, you can sample rows in the DataFrame by specifying the percentage and whether you want it to be sampled with or without replacement.
```python
# randomSplit(proportion, seed): returns a list of DFs
dfs = df.randomSplit([0.3, 0.7], 2)

# sample(withReplacement, fraction, seed)
df_sample = df.sample(False, .3, 2).count() # 30% of original DF
```

#### Handling Null Values 
```python
# Fill Nulls with specified values
df_fillna = df.fillna({'temperature': 68, 'wind': 6})
df_fillna = df.na.fill({'temperature': 68, 'wind': 6}) # Same

# Drop Rows where Specified Columns have Null values
df_dropna = df.dropna(['temperature', 'wind'])
```

#### Persisting / Caching Data  
Spark allows us to cache data in Spark to memory, disk, or both. We can also _unpersist_ data as well.
```python
from pyspark.storagelevel import StorageLevel

# Use default storage level (MEMORY_ONLY)
df.cache()
df.persist(StorageLevel.MEMORY_ONLY) # same

# Persist data w/ custom setting
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist data
df.unpersist()
```  

#### Repartitioning and Coalescing
Repartitioning will incur a full shuffle, while coalescing will not. Increasing the number of partitions can increase the number of parallelism for map & filter operations. 
```python
# Coalesce
df = df.coalesce(16)

# Repartition
df = df.repartition(10)
```  

#### Convert a DataFrame to Gobal/Temp View
If we want to work with our DF using SQL, we first need to create a Temporary View using the following command:
```python
df.createOrReplaceTempView('DF')
```

## Row & Column
### Working with Columns  
#### Referencing Columns
The simplest way to reference columns is to use either the `col()` or `column()` SQL functions. Explicit referencing using the DataFrame `.col()` method helps with Spark performance and is epecially helpful to remove ambiguitiy during joins.  
```python
from pyspark.sql.functions import col, column

col('columnName')
column('columnName') # Same thing
df.col('columnName') # Explicit referencing using DF method
```
#### Adding, Dropping, Casting, and Renaming Columns
```python
# New Col: First arg is new column name, Second arg is calculated value
newDF = df.withColumn('col2', 'col1' + 5)

# Drop column using .drop() method
newDF = df.drop('col2', 'col3', 'col5')

# Cast column to a different type


# Renamed Col: first arg is old column name, second arg is new column name
newDF = df.withColumnRenamed('col2', 'column_plus_five')
```  
#### Expressions
At the core level, a column is really an expression, which is a set of transformations on one or more values in a record on a dataframe. For example, `expr('columnName')` is equal to `col('columnName')`. We can use the `expr()` function to parse strings to apply additional transformations to a column.
```python
from pyspark.sql.functions import col, expr

col('columnName') - 5
expr('columnName - 5') # Same as above
expr('`column Name` - 5') # use backtick(`) for expressions referring to column names w/ spaces
```
### Working with Rows/Records 
```python
from pyspark.sql import Row

# Retrieve the first record, in Row format from df
df.first()

# Create a Row
my_row = Row('Hello', None, 1, False)

# Access items from the Row
my_row[0] # 'Hello'
```

_Source: SDG (Ch 5. Basic Structured Operations - "Records and Rows")_

## Spark SQL Functions
There are several additional functions that you can import from the [pyspark.sql.functions](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#module-pyspark.sql.functions) module and apply to transform columns in your DataFrame. Refer to SDG (Ch 5. Basic Structured Operations) as well.

#### Literal Values
To pass explicit values to Spark, we need to convert python's literal value into a Spark value using `lit`.
```python
from pyspark.sql.functions import lit

df.select(lit(1).alias('One')).show()
```

#### String Functions  
Change the character case
```
from pyspark.sql.functions import lower

df = df.withColumn('lowerCol', lower(col('col')))
```
Replace characters within a string
```
from pyspark.sql.functions import regexp_replace

# This will replace all instances of ":" with "_"
regexp_replace(col("newCol"), ":", "_")

```
Concatenate strings
```
from pyspark.sql.functions import concat

newDF = df.withColumn('concatcol', concat(col('col1'), col('col2')))
```  

#### Math Functions
```python
from pyspark.sql.functions import cos, floor, log

cosVal = cos(0) # 1.0

# add more math functions

```

#### Misc Functions
```python
from pyspark.sql.functions import sha1, sha2, md5, crc32

# CRC32 returns a cyclic redundancy check value as bigint
newDF = df.withColumn('hash', crc32('mycol'))

# MD5 returns 128-bit checksum as hex string
newDF = df.withColumn('hash', md5('mycol'))

# SHA-1
newDF = df.withColumn('hash', sha1('mycol'))

# SHA-2 returns checksum of SHA-2 family. Supports SHA-224, SHA-256, SHA-384, and SHA-512
newDF = df.withColumn('hash', sha2('mycol', 256))


```
## Advanced Spark SQL
The following operations use their own classes, so the imports and syntax will be slightly different.

#### Window Functions  
As in SQL, we can perform calculations across a window of rows.
```python
from pyspark.sql.window import Window

windowSpec = Window.partititonBy('col')

win_df = df\
  .withColumn('windowCol', avg('someCol').over(windowSpec))
```
_Source: [pyspark.sql.Window](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.Window)_

#### UDF Functions  
UDFs are always slower than built-in functions because:
* built-in functions are written in highly-optimized Scala
* Spark's optimization engine, the Catalyst Optimizer, knows the objective of built-in functions so it can optimize the execution of your code by changing the order of your tasks
* There is no serialization cost at the time of running a built-in function    

UDFs should only be used when there is no clear way of accomplishing the task using built-in functions. Register the function as a UDF by designating the following:

* A name for access in Python (`manualSplitPythonUDF`)
* A name for access in SQL (`manualSplitSQLUDF`)
* The function itself (`manual_split`)
* The return type for the function (`StringType`)  

Apply the UDF by using it just like any other Spark function.
```python
from pyspark.sql.types import StringType

def manual_split(x):
  return x.split("e")

manualSplitPythonUDF = spark.udf.register("manualSplitSQLUDF", manual_split, StringType())

newDF = df.withColumn('funCol', manualSplitPythonUDF('inputCol'))
```
UDFs can also produce complex output types by first creating a schema and then setting that schema as the output type.
```python
from pyspark.sql.types import FloatType, StructType, StructField

# Create the schema
mathOperationsSchema = StructType([
  StructField("sum", FloatType(), True), 
  StructField("multiplication", FloatType(), True), 
  StructField("division", FloatType(), True) 
])

# Define the function
def manual_math(x, y):
  return (float(x + y), float(x * y), x / float(y))

# Register the UDF
manualMathPythonUDF = spark.udf.register("manualMathSQLUDF", manual_math, mathOperationsSchema)

# Apply the UDF and display output
newDF = df.withColumn('sum', manualMathPythonUDF('col1', 'col2'))
display(newDF)
```
We can also employ vectorized Python UDFs (aka Pandas UDF), which alleviates some of the serialization and invocation overhead of Python UDFs.
```python
from pyspark.sql.functions import pandas_udf, PandasUDFType

# Decorator syntax to designate a Pandas UDF
@pandas_udf('double', PandasUDFType.SCALAR)
def pandas_plus_one(v):
    return v + 1

# Apply the UDF and display output
newDF = df.withColumn('id_transformed', pandas_plus_one("id"))
display(newDF)
```  
_Source: [pyspark.sql.UDFRegistration](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.UDFRegistration)_
