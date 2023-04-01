from testBase.QAEnv.filterMGCollection import *


def test_P2_027_unitType_2():
    filter = MGFilter()

    unitType_pointOfCare_map = {}
    pointOfCareLst_unitTax = []

    docs_unitTax = filter.unitTaxonomy_PETestClient()

    for doc in docs_unitTax:
        _id = doc.get("_id")
        print(doc.get('customerIds'))
        customerIds = "{}".format(*doc.get('customerIds'))
        print(_id, customerIds)
        unitType_unitTax = doc.get('unitType')

        pointOfCareLst_unitTax.append(customerIds)

        if unitType_unitTax not in unitType_pointOfCare_map:
            unitType_pointOfCare_map[unitType_unitTax] = [customerIds]
        else:
            unitType_pointOfCare_map[unitType_unitTax].append(customerIds)

    print(unitType_pointOfCare_map)

    docs = filter.extractedAdt_screenquality_pass_unitType_exists_PETestClient()

    for doc in docs:
        pointOfCare = doc.get('pointOfCare')
        unitType = doc.get('unitType')
        visitNumber = doc.get('visitNumber')
        print(visitNumber, unitType, pointOfCare)

        # assert pointOfCare in unitType_pointOfCare_map[unitType]








