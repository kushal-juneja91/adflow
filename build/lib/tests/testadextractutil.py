from tests.commontestutil import PySparkTest
import unittest
import os

class TestAdExtract(PySparkTest):

    def test_getdata(self):
        file=os.getcwd()+'/sampleloads.json'
        df=self.adextract.getData(file,'local', self.spark)
        assert df.count()==3

if __name__ == '__main__':
    unittest.main()

