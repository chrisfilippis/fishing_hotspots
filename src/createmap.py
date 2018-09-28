from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from scipy.spatial import distance
import math as math
from pyspark.sql import SQLContext
import gmplot
from os import listdir
import pandas
import os.path

def get_decimal(_text, step_value):
    return int(_text) * step_value

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

    create_heatmap_from_points(latitudes, longitudes, new_path + 'heatmap.html')

step_lat = 0.01
step_lon = 0.01
step_time = 120
csv_file_path = 'file:///C:/Users/filippisc/Desktop/project/data/nari_dynamic.csv'
top_k = 15000
count_data = 0
result_path = 'file:///C:/Users/filippisc/Desktop/Spark_Data/results'
create_html_from_csv(result_path)