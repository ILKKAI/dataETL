from database._mongodb import MongoClient


def data_shuffle(data):

    return data


if __name__ == '__main__':
    main_mongo = MongoClient(entity_code="WD_JT_GJ_GJWZD_CD", mongo_collection="WD_JT_GJ")
    data_list = main_mongo.main()
    for data in data_list:
        re_data = data_shuffle(data)
        print(re_data)