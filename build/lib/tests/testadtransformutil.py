from tests.commontestutil import PySparkTest
import unittest
import os

class TestAdTransform(PySparkTest):

    def util_joineventtest(self):
        file = os.getcwd() + '/firstevent.json'
        df = self.adextract.getData(file, 'local', self.spark)
        file1 = os.getcwd() + '/secondevent.json'
        df1 = self.adextract.getData(file1, 'local', self.spark)
        return (df, df1)

    def util_firstvisttest(self):
        file = os.getcwd() + '/getFirstVisitByTimeStamp.json'
        df = self.adextract.getData(file, 'local', self.spark)
        df1 = self.adtransform.getFirstVisitdByTimestamp(df)
        return (df, df1)

    def test_cleannullvisitor(self):
        file=os.getcwd()+'/cleanNullVisitors.json'
        df=self.adextract.getData(file,'local', self.spark)
        df.show()
        assert df.count() == 2
        df1=self.adtransform.cleanNullVisitors(df)
        assert df1.count() == 1

    def test_getFirstVisitdByTimestamp(self):
        df,df1=self.util_firstvisttest()
        print(df1.select("id").collect()[0]['id'])
        assert df1.select("id").collect()[0]['id']=="1b196775-c762-472a-a6ff-38c833e62d8f"

    def test_getNthVisitDf(self):
        df,df1=self.util_firstvisttest()
        df2 = self.adtransform.getNthVisitDf(df, df1)
        assert df2.count() == 1

    def test_substractCurrentEvents(self):
        df, df1 = self.util_firstvisttest()
        df2 = self.adtransform.substractCurrentEvents(df, df1)
        df2.show()
        assert df2.count() == 4

    def test_getSecondEventByTimestamp(self):
        df, df1 = self.util_firstvisttest()
        df2 = self.adtransform.substractCurrentEvents(df, df1)
        df2.show()
        assert df2.count() == 4

    def test_joinVisitEvents(self):
        event1,event2 = self.util_joineventtest()
        event_result=self.adtransform.joinVisitEvents(event1,event2)
        event_result.show()

    def test_transform(self):
        file = os.getcwd() + '/testtransform.json'
        df=self.adextract.getData(file,'local', self.spark)
        df1=self.adtransform.transform(df)
        print('final transformed df')
        df1.show(20,False)
        assert df1.count()==3

if __name__ == '__main__':
    unittest.main()

