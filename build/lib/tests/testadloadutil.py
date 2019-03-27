from tests.commontestutil import PySparkTest
import unittest
import os

class TestAdLoad(PySparkTest):
    def test_writedata(self):
        file = os.getcwd() + '/getFirstVisitByTimeStamp.json'
        df = self.adextract.getData(file, 'local', self.spark)
        self.filesave.write_data(df,'local','.','json')

if __name__ == '__main__':
    unittest.main()
