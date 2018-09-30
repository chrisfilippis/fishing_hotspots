from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from scipy.spatial import distance
import math as math
from pyspark.sql import SQLContext
import gmplot
from os import listdir
import pandas
import os.path

app_name = "Fishing hot spot app"

def result_path():
    return 'file:///spark/spark-2.2.1-bin-hadoop2.7/fish/resutls'
    # return 'file:///C:/Users/filippisc/Desktop/project/fishing_hotspots/data/result'

def csv_file_path():
    return 'hdfs:///data/fish/vessels_s.csv'
    # return 'file:///C:/Users/filippisc/Desktop/project/fishing_hotspots/data/vessels.csv'
    
def fishing_vessels_csv_file_path():
    return 'hdfs:///data/fish/fish.csv'
    # return 'file:///C:/Users/filippisc/Desktop/project/fishing_hotspots/data/fish.csv'

def get_decimal(_text, step_value):
    return int(_text) * step_value


def minutes_batch(unix_timestamp, min_t, step):
    step_seconds = 60 * step
    difference = unix_timestamp - min_t
    return int(difference / step_seconds)


def ceil_val(lat, step):
    return math.ceil(divide_val(lat, step))


def divide_val(lat, step):
    return lat / step


def get_spark_session():
    return SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()


def get_weight(point):
    return distance.euclidean((0, 0, 0), point)


def print_formatted(points_to_print, top=20, key=None):

    if key is None:
        for pri in points_to_print.top(top):
            print pri
    else:
        for pri in points_to_print.top(top, key=key):
            print pri


def transform_with_weight(s, min_tim, time_step):
    f_trans = (minutes_batch(s[0], min_tim.value, time_step), divide_val(s[1], step_lat), divide_val(s[2], step_lon), s[3])
    s_trans = (f_trans[0], f_trans[1], int(math.ceil(f_trans[1])), f_trans[2], int(math.ceil(f_trans[2])), f_trans[3])
    t_trans = (s_trans[0], s_trans[2], s_trans[4], get_weight((s_trans[0], s_trans[1], s_trans[3])), s_trans[5])
    return t_trans


# return lat_lon_time
def get_key(line, part1=1, part2=2, part3=0):
    return str(line[part1]) + '_' + str(line[part2]) + '_' + str(line[part3])


def handle_accumulators(x, _sum_x, _sum_x2):
    _sum_x.add(x[1])
    _sum_x2.add(math.pow(x[1], 2))


def get_min_max(init_rdd, index):
    return init_rdd.map(lambda x: x[index]).min(), init_rdd.map(lambda x: x[index]).max()


def get_direct_neighbor_ids(cell, t_min, t_max, ln_min, ln_max, lt_min, lt_max, cell_xi):
    key_parts = cell.split("_")
    lat, lon, time = int(key_parts[0]), int(key_parts[1]), int(key_parts[2])
    result_tuples = []

    lat_from = lat if lt_min == lat else lat - 1
    lat_to = lat if lt_max == lat else lat + 1

    lon_from = lon if ln_min == lon else lon - 1
    lon_to = lon if ln_max == lon else lon + 1

    time_from = time if t_min == time else time - 1
    time_to = time if t_max == time else time + 1

    for x in xrange(lat_from, lat_to + 1):
        for y in xrange(lon_from, lon_to + 1):
            for z in xrange(time_from, time_to + 1):
                if not (lat == x and lon == y and time == z):
                    result_tuples.append((str(x) + "_" + str(y) + "_" + str(z), cell_xi))

    return result_tuples


def get_getisord(cell, sumxi, n, large_x, large_s, t_min, t_max, ln_min, ln_max, lt_min, lt_max):

    nci = len(get_direct_neighbor_ids(cell, t_min, t_max, ln_min, ln_max, lt_min, lt_max, 0))

    sqrt_val = ((n * nci) - math.pow(nci, 2)) / (n - 1)
    gi = (sumxi - (large_x * nci)) / (large_s * math.sqrt(sqrt_val))

    return cell, gi


def create_heatmap_from_points(latitudes, longitudes, html_path):

    gmap = gmplot.GoogleMapPlotter(latitudes[0], longitudes[1], 5)

    # gmap.plot(latitudes, longitudes, 'cornflowerblue', edge_width=10)
    # gmap.scatter(latitudes, longitudes, '#3B0B39', size=40, marker=False)
    # gmap.scatter(latitudes, longitudes, 'k', marker=True)
    gmap.heatmap(latitudes, longitudes)
    gmap.draw(html_path)


def find_csvs_inpath(path_to_dir, suffix=".csv"):
    filenames = listdir(path_to_dir)
    return [filename for filename in filenames if filename.endswith(suffix)]


def create_html_from_csv(path_to_dir):
    new_path = path_to_dir.replace('file:///', '').replace('/', '\\')
    file = new_path + '\\' + find_csvs_inpath(new_path)[0]

    fields = ['id', 'gi']

    data = pandas.read_csv(file, sep=',', header=1, names=fields)

    latitudes = []
    longitudes = []

    for index, row in data.iterrows():
        latitudes.append(get_decimal(row['id'].split('_')[1], step_lat))
        longitudes.append(get_decimal(row['id'].split('_')[0], step_lon))

    create_heatmap_from_points(latitudes, longitudes, new_path + '\heatmap.html')


def load_rdd(csv_path, fishing_vessels_csv_path):
    fishing_vessels = sc.textFile(fishing_vessels_csv_path).filter(lambda l: not l.startswith('maritime_area'))

    fishing_vessels_source = fishing_vessels\
        .map(lambda x: x.split(";")[5])\
        .filter(lambda x: x != '')

    initData = sc.textFile(csv_path).filter(lambda l: not l.startswith('sourcemmsi'))

    initSource = initData\
        .map(lambda x: x.split(","))

    init_source_join = initSource \
        .map(lambda x: (int(x[0]), (int(x[8]), float(x[6]), float(x[7]))))

    fishing_vessels_source_join = fishing_vessels_source.map(lambda x: (int(x), 'fish'))
    joined = init_source_join.leftOuterJoin(fishing_vessels_source_join)    
    filtered_join = joined.filter(lambda x: not x[1][1] is None)
    return filtered_join.map(lambda x: (int(x[1][0][0]), float(x[1][0][1]), float(x[1][0][2]), int(x[0])))

sparkSession = get_spark_session()
sc = sparkSession.sparkContext
sqlContext = sparkSession
step_lat = 0.003
step_lon = 0.003
step_time = 120
count_data = 0

result_path = result_path()
csv_file_path = csv_file_path()
fishing_vessels_csv_file_path = fishing_vessels_csv_file_path()

acc_number_of_cells = sc.accumulator(0)
acc_sum_x = sc.accumulator(0)
acc_sum_x2 = sc.accumulator(0)
count_data = sc.accumulator(0)

#=62%
# int: time, float: lat, float: lon, int: id
source = load_rdd(csv_file_path, fishing_vessels_csv_file_path)

# find the min date
broad_time_min = source\
     .map(lambda x: x[0]).min()

broadcast_min_time = sc.broadcast(broad_time_min)

# time, lat, lon, xi,  id
structured_weighted_data = source \
    .map(lambda x: transform_with_weight(x, broadcast_min_time, step_time)) \
    .persist(StorageLevel.MEMORY_AND_DISK)

# find the min / max longitude
lon_min, lon_max = get_min_max(structured_weighted_data, 2)
lon_range = lon_max - lon_min

# find the min / max latitude
lat_min, lat_max = get_min_max(structured_weighted_data, 1)
lat_range = lat_max - lat_min

# find the min / max date
time_min, time_max = get_min_max(structured_weighted_data, 0)
time_range = time_max - time_min

n = lat_range * lon_range * time_range

# number of points in 3D cells
keyValue_data = structured_weighted_data\
    .map(lambda x: (get_key(x), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .filter(lambda x: x[1] > 1) \

# calculate xi foreach cell
keyValue_weighted_data = structured_weighted_data \
    .map(lambda x: (get_key(x), x[3])) \
    .reduceByKey(lambda x, y: x + y) \

# calculate the sum of xi and xi^2 using accumulator sum_x and acc_sum_x2
keyValue_weighted_data.foreach(lambda x: handle_accumulators(x, acc_sum_x, acc_sum_x2))

# find the number of cells using accumulator number_of_cells
source.foreach(lambda x: acc_number_of_cells.add(1))

source.foreach(lambda x: count_data.add(1))

# get values
number_of_cells = acc_number_of_cells.value
sum_x = acc_sum_x.value
sum_x2 = acc_sum_x2.value

# calculate X
X = sum_x / n

# calculate S
S = math.sqrt((sum_x2 / n) - math.pow(X, 2))

keyValue_with_neighbor_weights = keyValue_weighted_data\
    .flatMap(lambda line: get_direct_neighbor_ids(line[0], time_min, time_max, lon_min, lon_max, lat_min, lat_max, line[1])) \
    .reduceByKey(lambda x, y: x + y)

# cell, cell_xi, n, large_x, large_s, t_min, t_max, ln_min, ln_max, lt_min, lt_max, cell_xi
getis_ord_keyValue = keyValue_with_neighbor_weights\
    .map(lambda line: get_getisord(line[0], line[1], n, X, S, time_min, time_max, lon_min, lon_max, lat_min, lat_max))

getis_dataFrame = sqlContext.createDataFrame(getis_ord_keyValue, ['id', 'gi'])

getis_dataFrame.sort(['gi'], ascending=[0]).limit(int(count_data.value * 0.03)).repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(result_path)
create_html_from_csv(result_path)
sparkSession.stop()