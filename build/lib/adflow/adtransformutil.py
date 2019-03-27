import sys
from pyspark.sql.functions import isnull, col, lit, first
from pyspark.sql.window import Window

class AdTransformUtil():
    def transform(self, adsDf):
        adsDfRemovedAnonymusVisit = self.cleanNullVisitors(adsDf)
        first_events = self.getFirstVisitdByTimestamp(adsDfRemovedAnonymusVisit)
        adsFirstEvents=self.getNthVisitDf(adsDfRemovedAnonymusVisit,first_events)
        adsRemainingEvents=self.substractCurrentEvents(adsDfRemovedAnonymusVisit, first_events)
        second_events=self.getSecondEventByTimestamp(adsRemainingEvents)
        adsSecondEvents = self.getNthVisitDf(adsRemainingEvents, second_events)
        pageflowdf = self.joinVisitEvents(adsFirstEvents,adsSecondEvents)
        return pageflowdf


    def cleanNullVisitors(self, adsDf):
        totalRecords = adsDf.count()
        adsFilteredVisitors = adsDf.filter(~isnull("visitorId"))
        totalRecordsNull = adsFilteredVisitors.count()
        print("Total number of records::" + str(totalRecords))
        print("Removed visitors with null visitorId count::" + str(totalRecordsNull))
        return adsFilteredVisitors

    def getFirstVisitdByTimestamp(self, adsDf):
        windowVisitor = Window.partitionBy(adsDf['visitorId']).orderBy(adsDf['timestamp'].asc()).rangeBetween(-sys.maxsize, sys.maxsize)
        first_visit = first(adsDf["id"]).over(windowVisitor)
        first_visit_events = adsDf.select(first_visit.alias("id")).distinct()
        return first_visit_events

    def getNthVisitDf(self, adsDf, eventsDf):
        adsFilteredVisitorsNthtVisit = adsDf.join(eventsDf, "id")
        return adsFilteredVisitorsNthtVisit

    def substractCurrentEvents(self, adsDf,eventsDf):
        adsFilteredVisitorsSubstract = adsDf.join(eventsDf, "id", "leftanti")
        return adsFilteredVisitorsSubstract

    def getSecondEventByTimestamp(self, remainAdsDf):
        windowVisitorSecond = Window.partitionBy(remainAdsDf['visitorId']).orderBy( remainAdsDf['timestamp'].asc()).rangeBetween(-sys.maxsize, sys.maxsize)
        second_visit = first(remainAdsDf["id"]).over(windowVisitorSecond)
        second_visit_events = remainAdsDf.select(second_visit.alias("id")).distinct()
        return second_visit_events

    def joinVisitEvents(self, adsDfVisitOne, adsDfVisitTwo):
        adsDfVisitTwo = adsDfVisitTwo.withColumnRenamed("visitorId", "visitorIdSecond").withColumnRenamed("pageUrl", "nextPageUrl")
        flowdf = adsDfVisitOne.join(adsDfVisitTwo, (
                    adsDfVisitOne.visitorId == adsDfVisitTwo.visitorIdSecond),
                                                    "left").select(adsDfVisitOne.id,
                                                                   adsDfVisitOne.timestamp,
                                                                   adsDfVisitOne.type,
                                                                   adsDfVisitOne.visitorId,
                                                                   adsDfVisitOne.pageUrl,
                                                                   adsDfVisitTwo.nextPageUrl)
        return flowdf







