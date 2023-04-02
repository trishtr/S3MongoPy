def test_create_new_dict():
    test_dict = {

        'mainKey1': ['a', 'b', 'a', 'c', 'b'],
        'mainKey2': ['a', 'a', 'a', 'b', 'b']

    }

    new_dict = {}

    for v in test_dict.keys():
        print(test_dict[v])
        d = {x: test_dict[v].count(x) for x in test_dict[v]}
        new_dict[v] = d

    print(new_dict)











