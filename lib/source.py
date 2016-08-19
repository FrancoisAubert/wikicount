import glob
import urllib
from os.path import dirname, join
from datetime import datetime, timedelta

from wikicount.lib.bundle import Bundle


class Source(object):

    def __init__(self, *args, **conf):
        self.start_datetime = args[0]
        self.end_datetime = args[1]
        self.base_source_path = conf["base_source_path"]
        self.local_base_directory = conf["local_base_directory"]
        self.blacklist_url = conf["blacklist_url"]

    def candidate_bundles(self):
        time_delta = self.end_datetime - self.start_datetime
        assert(time_delta.total_seconds() >= 0)
        datetime_list = [self.start_datetime + timedelta(0, x*3600)
                         for x in range(1+int(time_delta.total_seconds())/3600)]
        return [Bundle(_datetime, self.base_source_path) for _datetime in datetime_list]

    def build_source(self):
        candidate_bundles = self.candidate_bundles()
        self._save_bundles(candidate_bundles)
        self._save_blacklist()

    def fetch_source(self):
        files = [join(self.local_base_directory, bundle.bundle_id)
                 for bundle in self.candidate_bundles()]
        return files

    def fetch_blacklist(self):
        return join(self.local_base_directory, self.blacklist_url.split("/")[-1])

    def _save_bundles(self, bundles):
        saved_bundles = [bundle_path.split("/")[-1]
                         for bundle_path in glob.glob(join(self.local_base_directory, "*.gz"))]
        for bundle in bundles:
            if bundle.bundle_id not in saved_bundles:
                bundle.save_bundle(self.local_base_directory)

    def _save_blacklist(self):
        print "Saving blacklist..."
        urllib.urlretrieve(self.blacklist_url, join(self.local_base_directory, self.blacklist_url.split("/")[-1]))


if __name__ == '__main__':
    main()
