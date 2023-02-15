import pytest
from testBase.QAEnv.filterMGCollection import MGFilter

class Test_parsedHL7_Metadata:


    def test_eventType(self):
        self.filter = MGFilter()
        docs = self.filter.parsedHL7_latest()
        print('Event Type : ')
        for doc in docs :
            eventType = doc.get('eventType')
            print(eventType)