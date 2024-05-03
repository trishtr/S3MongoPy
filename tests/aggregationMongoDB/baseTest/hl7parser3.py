import json
from hl7apy import parser
from hl7apy.parser import parse_message
from hl7apy.parser import parse_segments
from hl7apy.parser import parse_fields
from hl7apy.exceptions import UnsupportedVersion
from hl7apy.parser import parse_segment
from collections import OrderedDict
from hl7apy.core import Group, Segment, Message
from hl7apy.utils import iteritems
from hl7apy import load_reference
from hl7apy.v2_5_1 import FT


def parser(mirth_message):
    
  
    raw_hl7 = mirth_message.get('payload').get('eventData').get('HL7')
    eventType = mirth_message.get('eventType')
    eventSubType = mirth_message.get('eventSubType')
    eventId = mirth_message.get('eventId')
    eventSource = mirth_message.get('eventSource')
    clientId = mirth_message.get('clientId')
    locationId = mirth_message.get('locationId')
    timestamp = mirth_message.get('timestamp')
 


    # print(raw_hl7)


    # hl7_json = hl7_to_json(raw_hl7)
    # print("Parsed HL7 message as JSON:")
    # print(hl7_json)
    #  __________________________
   
    try:
        parsed = parse_message(raw_hl7, find_groups = True, validation_level = 2)
     
    except UnsupportedVersion:
        parsed = parse_message(raw_hl7.replace("n", "r"))

    messageType = parsed.MSH.MSH_9.MSG_1.value
    print(messageType)
    triggerEventType = parsed.MSH.MSH_9.MSG_2.value
   
    mappings = {"SendingApplication":["MSH_3_HD_1"], 
                "SendingFacility":["MSH_4_HD_1"],
                "ReceivingApplication": ["MSH_5"],
                "ReceivingFacility":  ["MSH_6_HD_1"],
                "MessageDateTime":["MSH_7_TS_1"], 
                "MessageControlID":["MSH_10_ST"],
                "ProcessingID": ["MSH_11_PT_1"], 
                "VersionID": ["MSH_12_VID_1"],
                "EventTypeCode": ["EVN_1_ID"],
                "RecordedDateTime":  ["EVN_2_TS_1"],
                "PatientClass":    ["PV1_2_IS"],
                "PointOfCare": ["PV1_3_PL_1"],
                "Room": ["PV1_3_PL_2"],
                "Bed": ["PV1_3_PL_3"],
                "AssignedFacility": ["PV1_3_PL_4"],
                "AdmissionType": ["PV1_4_IS"], 
                "HospitalService": ["PV1_10_IS"],
                "AdmitSource": ["PV1_14_IS"],
                "PatientType": ["PV1_18_IS"], 
                "VisitNumber": ["PV1_19_CX_1", "PID_18_CX_1"] , 
                "AdmitDateTime": ["PV1_44_TS_1"], 
                "DischargeDateTime":["PV1_45_TS_1"]}
    
    # print(parsed.msh.msh_9_1.value)

    # Testing2: 
    # print(parsed.pv1.pv1_36.IS)
    # print(parsed.evn.evn_1.value)
    # print(parsed.msh.msh_4.hd_1.value)

    print(f"***********test 2: for print all the segments**********")
   

    field_dict = OrderedDict()
    if messageType == 'ADT':
        for segment in parsed.children:
            print(segment)
            if isinstance(segment, Segment):
                for field in segment.children:
                    print(field, field.value)
                    
                    for component in field.children:
                        print(component, component.value)
                        key = f"{field.name}_{component.name}"
                        value = component.value
                        field_dict[key] = value

            if isinstance(segment, Group):
                for group in segment.children:
                    for group_segment in group.children:
                        for field in group_segment.children:
                            for component in field.children:
                                key = f"{field.name}_{component.name}"
                                value = component.value
                                field_dict[key] = value

        print(field_dict)
   

    complete_message = {
        "eventType": eventType,
        "eventSubType": eventSubType,
        "eventId": eventId,
        "clientId": clientId,
        "eventSource": eventSource,
        "locationId": locationId,
        "timestamp": timestamp,
        "payload":{
            "eventData":{
                "ADT":{
                    "types": messageType,
                    "fields": {}
                }
            }
        }

    }
       

    final_dict = OrderedDict()

    for k1, v1 in field_dict.items():
        for k2,v2 in mappings.items():
            if k1 in v2:
                final_dict[k2] = v1
                break
        
    # print(final_dict)

    
    complete_message["payload"]["eventData"]["ADT"]["fields"].update(final_dict)
    # print(complete_message)
    

    final_json = json.dumps(complete_message)
    print(final_json)
    

# other tests
# def segment_to_json(segment):
#     segment_json = OrderedDict()
    
#     for field in segment.children:
#         field_name = field.name
        
#         # Check if the field has multiple repetitions
#         repetitions = []
#         for repetition in field.children:
#             components = []
#             for component in repetition.children:
#                 subcomponents = [sc.to_er7() for sc in component.children]
#                 # If there are subcomponents, add them
#                 if subcomponents:
#                     components.append(subcomponents)
#                 else:
#                     components.append(component.to_er7())
#             repetitions.append(components)

#         # If there's only one repetition and one component, return a simple value
#         if len(repetitions) == 1 and len(repetitions[0]) == 1:
#             segment_json[field_name] = repetitions[0][0]
#         else:
#             segment_json[field_name] = repetitions
    
#     return segment_json

# Function to convert an entire HL7 message to JSON structure
# def hl7_to_json(hl7_message):
#     parsed_message = parse_message(hl7_message)
#     message_json = OrderedDict()
    
#     # Convert each segment to JSON
#     for segment in parsed_message.children:
#         if isinstance(segment, Segment):
#             segment_name = segment.name
#             message_json[segment_name] = segment_to_json(segment)

#     return json.dumps(message_json, indent=4)
   

if __name__== '__main__':
    a01 = ''
    parser(a01)