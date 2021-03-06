from os.path import join, dirname
from pyspark import SparkContext
from pyspark.rdd import RDD

from wikicount.lib.utils import takeOrderedByKey


class Processor(object):

    RDD.takeOrderedByKey = takeOrderedByKey

    def __init__(self, source, destination=None):
        self.source = source
        self.destination = destination
        self.sc = SparkContext()

    def process(self):
        self.source.build_source()
        data_rdd = self._preprocess_data(self._get_data_rdd())
        blacklist_rdd = self._preprocess_blacklist(self._get_blacklist_rdd())
        rdd = self._filter_black_listed_items(data_rdd, blacklist_rdd)
        clean_rdd = rdd.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x))
        return clean_rdd.takeOrderedByKey(25,  sortValue=lambda x: x[1], reverse=True).flatMap(lambda x: x[1]).collect()

    def _get_data_rdd(self):
        files = self.source.fetch_source()
        return self.sc.union([self.sc.textFile(f) for f in files]).repartition(self.sc.defaultParallelism * 2)

    def _get_blacklist_rdd(self):
        return self.sc.textFile(self.source.fetch_blacklist())

    def _preprocess_data(self, rdd):
        return rdd.map(self._preprocess_data_line)

    def _preprocess_blacklist(self, rdd):
        return rdd.map(self._preprocess_blacklist_line)

    def _filter_black_listed_items(self, data_rdd, blacklist_rdd):
        return data_rdd.subtractByKey(blacklist_rdd)

    @staticmethod
    def _preprocess_data_line(line):
        splited_line = line.split(" ")
        try:
            return (((splited_line[0], splited_line[1])), int(splited_line[3]))
        except IndexError:
            return (((splited_line[0], splited_line[1])), 0)

    @staticmethod
    def _preprocess_blacklist_line(line):
        splited_line = line.split(" ")
        return ((splited_line[0], splited_line[1]), 0)

    @staticmethod
    def send_results(results, start_datetime, end_datetime):
        RESULTS_PATH = join(dirname(__file__), "../files")
        results_file = join(RESULTS_PATH, str(start_datetime) + "-" + str(end_datetime))
        with open(results_file, 'w+') as infile:
            for result in results:
                try:
                    infile.write("domain : {}, page : {}, pagecount : {} \n".format(
                    str(result[0][0]).encode('utf-8'),
                    str(result[0][1]).encode('utf-8'),
                    str(result[1]).encode('utf-8')))
                except UnicodeEncodeError:
                    continue
