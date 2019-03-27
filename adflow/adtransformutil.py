"""
Applies a series of transformations to the data to find the first and second events and
save the final data
"""
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

    ##Remove the records with null visitorids
    def cleanNullVisitors(self, adsDf):
        totalRecords = adsDf.count()
        adsFilteredVisitors = adsDf.filter(~isnull("visitorId"))
        totalRecordsNull = adsFilteredVisitors.count()
        print("Total number of records::" + str(totalRecords))
        print("Removed visitors with null visitorId count::" + str(totalRecordsNull))
        return adsFilteredVisitors

    ##Gets the first Visit by windowing. Both the Visit functions can be combined into one. But for the sake of easier understanding it is
    ##divided into two
    def getFirstVisitdByTimestamp(self, adsDf):
        windowVisitor = Window.partitionBy(adsDf['visitorId']).orderBy(adsDf['timestamp'].asc()).rangeBetween(-sys.maxsize, sys.maxsize)
        first_visit = first(adsDf["id"]).over(windowVisitor)
        first_visit_events = adsDf.select(first_visit.alias("id")).distinct()
        return first_visit_events

    ##Gets the dataframe using the events
    def getNthVisitDf(self, adsDf, eventsDf):
        adsFilteredVisitorsNthtVisit = adsDf.join(eventsDf, "id")
        return adsFilteredVisitorsNthtVisit

    ##Gets the remaining data by removing all the events found in NthVisit
    def substractCurrentEvents(self, adsDf,eventsDf):
        adsFilteredVisitorsSubstract = adsDf.join(eventsDf, "id", "leftanti")
        return adsFilteredVisitorsSubstract

    ##Same as getFirstVisitEventByTimestamp
    def getSecondEventByTimestamp(self, remainAdsDf):
        windowVisitorSecond = Window.partitionBy(remainAdsDf['visitorId']).orderBy( remainAdsDf['timestamp'].asc()).rangeBetween(-sys.maxsize, sys.maxsize)
        second_visit = first(remainAdsDf["id"]).over(windowVisitorSecond)
        second_visit_events = remainAdsDf.select(second_visit.alias("id")).distinct()
        return second_visit_events

    ##Join the two events dfs to generate the output and emit that to the load phase
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







