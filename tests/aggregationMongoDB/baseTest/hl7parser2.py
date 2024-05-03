from hl7apy.parser import parse_message

def test_PV1():
    
    hl7 = 'version2.5.1. check the datatype structures'

    parsed_message = parse_message(hl7)

    # print(parsed_message.children)
    segments = parsed_message.children
    
    # NEW
    print(parsed_message.pv1.children)
    print(parsed_message.pv1.pv1_3.children)
    print(parsed_message.pv1.pv1_3.point_of_care.value)
    print(parsed_message.pv1.pv1_3.assigning_authority_for_location.value)
    print('Facility_ or AssignedLocation: ' +  parsed_message.pv1.pv1_3.facility.value)

    # OLD:
    # print(parsed_message.pv1.pv1_3.point_of_care_id.value)
    # print(parsed_message.pv1.pv1_3.facility_hd.value)
