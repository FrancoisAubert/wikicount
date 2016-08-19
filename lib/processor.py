from os.path import join
from pyspark import SparkContext


class Processor(object):

    def __init__(self, source, destination=None):
        self.source = source
        self.destination = destination
        self.sc = SparkContext()

    def process(self):
        self.source.build_source()
        data_rdd = self._preprocess_data(self._get_data_rdd())
        blacklist_rdd = self._preprocess_blacklist(self._get_blacklist_rdd())
        rdd = self._filter_black_listed_items(data_rdd, blacklist_rdd)
        return rdd.reduceByKey(lambda x, y: x + y).takeOrdered(25, key=lambda x: -x[1])

    def _get_data_rdd(self):
        files = self.source.fetch_source()
        return self.sc.union([self.sc.textFile(f) for f in files]).repartition(self.sc.defaultParallelism * 3)

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
    def send_results(self, results, start_datetime, end_datetime):
        RESULTS_PATH = join(dirname(__file__), "../files")
        results_file = join(RESULTS_PATH, str(start_datetime) + "-" + str(end_datetime))
        with open(results_file, 'r') as infile:
            for result in results:
                infile.write("domain : {}, page : {}, pagecount : {} \n".format(results[0][0],
                                                                                results[0][1],
                                                                                results[1]))
