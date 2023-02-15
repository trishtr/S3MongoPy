import pymongo

from testBase.QAEnv.filterMGCollection import *



class Test_extractedEvents:

    def test_extractEvents(self):

        self.filter = MGFilter()
        self.docs = self.filter.extractedAdtEventsFilter()

        test = {}
        for doc in self.docs:
            visitNumber = doc.get('visitNumber')
            eventTypeCode = doc.get('eventTypeCode')
            if visitNumber not in test:
                test[visitNumber] = [eventTypeCode]
            else:
                test[visitNumber].append(eventTypeCode)

        print(test)
























