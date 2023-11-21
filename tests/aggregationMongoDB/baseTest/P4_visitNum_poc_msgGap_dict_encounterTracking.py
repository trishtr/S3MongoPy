
def encounterTracking_dict_creator(P4_ETTestClient_EncounterTracking_docs):

    unit_count = {}
    final_dict = {}
    docs = P4_ETTestClient_EncounterTracking_docs

    for doc in docs:
        visitNumber = doc.get('visitNumber')
        tracking = doc.get('trackingStatus')

        for track in tracking:

            unitId = track.get('unit')

            if visitNumber not in unit_count:
                unit_count[visitNumber] = [unitId]
            else:
                unit_count[visitNumber].append(unitId)


    for v in unit_count.keys():

        count = {x:unit_count[v].count(x) for x in unit_count[v]}
        final_dict[v] = count



    print('Encounter Tracking___________: ')
    print(final_dict)

    return final_dict





