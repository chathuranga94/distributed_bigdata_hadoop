from pyspark import SparkContext, SparkConf
#from my-module import myfn

if __name__ == '__main__':
    conf = SparkConf().setAppName("app")
    sc = SparkContext(conf=conf)

    sc = sc.textFile("/home/bibi/Desktop/distributed.csv") 
    sc = sc.map(lambda line: line.split(",")) 
    sc = sc.map(lambda line: (int(line[0]),float(line[2]))) 
    sc = sc.combineByKey( lambda value: (value, 1),lambda x, value: (x[0] + value, x[1] + 1),lambda x, y: (x[0] + y[0], x[1] + y[1]))
    sc = sc.map(lambda (label, (value_sum, count)): (label, value_sum / count))
    print sc.collectAsMap();
