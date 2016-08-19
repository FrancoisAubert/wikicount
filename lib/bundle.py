import urllib
from os.path import join


class Bundle(object):

    def __init__(self, _datetime, base_path):

        self.base_path = base_path
        self.bundle_base_path = "{_base_path}/{_year}/{_year}-{_month}".format(
                                        _base_path=self.base_path,
                                        _year=_datetime.year,
                                        _month=str(_datetime.month).zfill(2),
                                        )
        self.bundle_id = "pagecounts-{_year}{_month}{_day}-{_hour}0000.gz".format(
                                 _year=_datetime.year,
                                 _month=str(_datetime.month).zfill(2),
                                 _day=str(_datetime.day).zfill(2),
                                 _hour=str(_datetime.hour).zfill(2)
                                 )

    def save_bundle(self, base_directory_path):
        print "Saving bundle {}...".format(self.bundle_id)
        urllib.urlretrieve(join(self.bundle_base_path, self.bundle_id),
                           join(base_directory_path, self.bundle_id))
