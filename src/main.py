from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import gmplot
import pandas

app_name = "Hot spot app"
master = "local[*]"

def get_spark_config():
    return SparkConf()\
        .setAppName(app_name)\
        .setMaster(master) \
        .set("spark.executor.cores", "4")\
        .set("spark.driver.memory", "4g")


def get_spark_context():
    configuration = get_spark_config()
    return SparkContext.getOrCreate(conf=configuration)

sc = get_spark_context()
sqlContext = SQLContext(sc)
csv_file_path = 'C:\Users\\filippisc\Desktop\project\data\[P1] AIS Data\\nari_dynamic.csv'

initSource = sc.textFile(csv_file_path)\
    .map(lambda x: x.split(","))