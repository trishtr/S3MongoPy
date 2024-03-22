from hl7apy.parser import parse_message

hl7  = ""
parsed_message = parse_message(hl7)

print(parsed_message.children)
segments = parsed_message.children


# print(segments[1].children)
# print(segments[1].children[3].children)
# [<Segment MSH>, <Segment EVN>, <Segment PID>, <Segment PV1>]
# [<Field EVN_1 (EVENT_TYPE_CODE) of type ID>, <Field EVN_2 (RECORDED_DATE_TIME) of type TS>, <Field EVN_4 (EVENT_REASON_CODE) of type IS>, <Field EVN_5 (OPERATOR_ID) of type XCN>]

# for segment in segments:
#     print("Segment:", segment.name)  # Print segment name

print(parsed_message.evn.operator_id.children)
# print(parsed_message.evn.value)
print('Assigning Location: ' + parsed_message.evn.operator_id.xcn_14.value)

print(parsed_message.pv1.assigned_patient_location.children)
# print('Assigned Patient Location : ' + parsed_message.pv1.assigned_patient_location.value)
print('Point Of Care :' + parsed_message.pv1.assigned_patient_location.point_of_care.value)

print('Facility :' + parsed_message.pv1.assigned_patient_location.facility.value)

print('Location Description :' + parsed_message.pv1.assigned_patient_location.location_description.value)

print('Assigning Authority For Location :' + parsed_message.pv1.assigned_patient_location.assigning_authority_for_location.value)


