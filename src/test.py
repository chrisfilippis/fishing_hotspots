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

app_name = "Hot spot app"
master = "local[*]"

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


# places   degrees          distance
# -------  -------          --------
# 0        1                111  km
# 1        0.1              11.1 km
# 2        0.01             1.11 km
# 3        0.001            111  m
# 4        0.0001           11.1 m
# 5        0.00001          1.11 m
# 6        0.000001         11.1 cm
# 7        0.0000001        1.11 cm
# 8        0.00000001       1.11 mm

sparkSession = get_spark_session()
sc = sparkSession.sparkContext
sqlContext = sparkSession
step_lat = 0.003
step_lon = 0.003
step_time = 120
top_k = 15000
count_data = 0
result_path = 'file:///C:/Users/filippisc/Desktop/Spark_Data/results'
csv_file_path = 'file:///C:/Users/filippisc/Desktop/project/data/nari_dynamic_sample.csv'
fishing_vessels_csv_file_path = 'file:///C:/Users/filippisc/Desktop/project/data/anfr.csv'

# result_path = 'file:///spark/spark-2.2.1-bin-hadoop2.7/results'
# csv_file_path = 'hdfs:///data/test/data_full.csv'
# csv_file_path = "C:\Spark_Data\million_bigdata.sample"

acc_number_of_cells = sc.accumulator(0)
acc_sum_x = sc.accumulator(0)
acc_sum_x2 = sc.accumulator(0)
count_data = sc.accumulator(0)



fishing_vessels = sc.textFile(fishing_vessels_csv_file_path).filter(lambda l: not l.startswith('maritime_area'))

fishing_vessels_source = fishing_vessels\
    .map(lambda x: x.split(";")[5])\
    .filter(lambda x: x != '')

initData = sc.textFile(csv_file_path).filter(lambda l: not l.startswith('sourcemmsi'))

initSource = initData\
    .map(lambda x: x.split(","))

init_source_join = initSource \
    .map(lambda x: (int(x[0]), (int(x[8]), float(x[6]), float(x[7]))))

fishing_vessels_source_join = fishing_vessels_source.map(lambda x: (long(x), 'fish'))

joined = init_source_join.leftOuterJoin(fishing_vessels_source_join)

filtered_join = joined.filter(lambda x: not x[1][1] is None)

print_formatted(filtered_join, 50)