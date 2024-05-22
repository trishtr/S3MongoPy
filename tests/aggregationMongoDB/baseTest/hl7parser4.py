import json
from hl7apy import parser
from hl7apy.parser import parse_message
from hl7apy.exceptions import UnsupportedVersion, HL7apyException
from collections import OrderedDict
from hl7apy.core import Group, Segment


class HL7Parser():

    def __init__(self, mirth_message):
        self.mirth_message = mirth_message
        self.raw_hl7 = mirth_message['payload']['eventData'].get('HL7')
        self.eventType = mirth_message.get('eventType')
        self.eventSubType = mirth_message.get('eventSubType')
        self.eventId = mirth_message.get('eventId')
        self.clientId = mirth_message.get('clientId')
        self.eventSource = mirth_message.get('eventSource')
        self.locationId = mirth_message.get('locationId')
        self.timestamp = mirth_message.get('timestamp')

        self.mappings = {
            "SendingApplication": ["MSH_3_HD_1"],
            "SendingFacility": ["MSH_4_HD_1"],
            "ReceivingApplication": ["MSH_5"],
            "ReceivingFacility": ["MSH_6_HD_1"],
            "MessageDateTime": ["MSH_7_TS_1"],
            "MessageControlID": ["MSH_10_ST"],
            "ProcessingID": ["MSH_11_PT_1"],
            "VersionID": ["MSH_12_VID_1"],
            "EventTypeCode": ["EVN_1_ID"],
            "RecordedDateTime": ["EVN_2_TS_1"],
            "PatientClass": ["PV1_2_IS"],
            "PointOfCare": ["PV1_3_PL_1"],
            "Room": ["PV1_3_PL_2"],
            "Bed": ["PV1_3_PL_3"],
            "AssignedFacility": ["PV1_3_PL_4"],
            "AdmissionType": ["PV1_4_IS"],
            "HospitalService": ["PV1_10_IS"],
            "AdmitSource": ["PV1_14_IS"],
            "VisitNumber": ["PV1_19_CX_1", "PID_18_CX_1"],
            "AdmitDateTime": ["PV1_44_TS_1"],
            "DischargeDateTime": ["PV1_45_TS_1"],
        }


    def parse_raw_hl7(self):
        
        try:
            parsed_message = parse_message(self.raw_hl7.replace("n", "r"), find_groups = True, validation_level=2)
        except UnsupportedVersion:
            print("Unsupported HL7 version encountered. Consider updating the parser or rechecking the message version")
            try: 
                parsed_message = parse_message(self.raw_hl7.replace("n", "r"), find_groups = False, validation_level = 1)
            except HL7apyException as he:
                print("Error while parsing HL7 message with a more flexible approach:")
                print(f"HL7apyException: {he}")
            except Exception as e:
                print("Unexpected error occurred during retry:")
                print(f"Exception: {e}")
        else:
            print("HL7 message parsed successfully:")
            print(parsed_message.to_er7())
            
        return parsed_message
    
    def set_default_OBX2_type(self, parsed_message):
        for segment in parsed_message.children:
            # Check if the segment is an OBX segment
            if segment.name == "OBX":
                obx_2_field = segment.obx_2
                if obx_2_field:
                    obx_2_field.data_type = 'ST'
    
    def extract_fields(self, parsed_message):
        field_dict = OrderedDict()
        for segment in parsed_message.children:
            if isinstance(segment, Segment):
                for field in segment.children:
                    self.extract_field_data(field, field_dict)
            elif isinstance(segment, Group):
                for group in segment.children:
                    for group_segment in group.children:
                        for field in group_segment.children:
                            self.extract_field_data(field, field_dict)
        return field_dict
    
    def extract_field_data(self, field, field_dict):
        for component in field.children:
            key = f"{field.name}_{component.name}"
            value = component.value
            field_dict[key] = value

    def create_final_dict(self, field_dict):
        final_dict = OrderedDict()
        for k1, v1 in field_dict.items():
            for k2, v2 in self.mappings.items():
                if k1 in v2:
                    final_dict[k2] = v1
                    break
        return final_dict

    def create_complete_message(self, field_dict):
        complete_message = {
            "eventType": self.eventType,
            "eventSubType": self.eventSubType,
            "eventId": self.eventId,
            "clientId": self.clientId,
            "eventSource": self.eventSource,
            "locationId": self.locationId,
            "timestamp": self.timestamp,
            "payload": {
                "eventData": {
                    "ADT": {
                        "types": self.eventType,
                        "fields": {}
                    }
                }
            }
        }
        
        final_dict = self.create_final_dict(field_dict)
        complete_message["payload"]["eventData"]["ADT"]["fields"].update(final_dict)
        return complete_message
    


    def to_json(self, complete_message):
        return json.dumps(complete_message, indent=2)
        
    

a01 = {"eventType":"INTAKE",
       "eventSubType":"HL7",
       "eventId":"",
       "clientId":"",
       "eventSource":"MIRTH",
       "locationId":"",
       "timestamp":"2024-05-01T18:55:06Z",
       "payload":
        {"eventData":
            {"HL7":"MSH|^~\\&|EPIC^1.2.840.114350.374^ISO........abc"}}}
parser = HL7Parser(a01)    
parsed_message = parser.parse_raw_hl7()
if parsed_message:
    # Extract fields from the parsed message
    field_dict = parser.extract_fields(parsed_message)
    
    # Create the complete message with additional data
    complete_message = parser.create_complete_message(field_dict)
    
    # Convert the complete message to JSON
    json_message = parser.to_json(complete_message)

    # Print the JSON representation of the message
    print("Parsed HL7 as JSON:")
    print(json_message)
else:
    print("Failed to parse the HL7 message.")