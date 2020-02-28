# -*- coding: utf-8 -*-

# 学校基本信息 SCHOOL_FINASSIST

# 已完成 887条 23条乱码数据未添加

import sys
import os

from hbase.ttypes import Mutation, BatchMutation
from database._hbase import HbaseClient
from scripts import GenericScript

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
# print(rootPath)
sys.path.append(rootPath[:-8])


class SchoolFinassistScript(object):
    # 初始化参数
    def __init__(self, entity_type="SCHOOL_FINASSIST"):
        self.entity_type = entity_type

    def data_shuffle(self, mongo_data_list, province_list, city_list, area_list):
        batch_list = list()

        for city in city_list:
            if city["NAME_"] == "县":
                city_list.remove(city)

        for data in mongo_data_list:
            mutation_list = list()
            # print(data)
            prov_n = None
            prov_c = None
            city_n = None
            city_c = None
            area_n = None
            area_c = None

            # 省级字段
            for prov in province_list:
                if prov["NAME_"] == data["AREA_"][:len(prov["NAME_"])]:
                    prov_n = prov["NAME_"]
                    prov_c = prov["CODE_"]

            # todo 乱码
            # if prov_c is None:
                # print(data)

            # 市级字段
            for city in city_list:
                if city["PARENT_"] == prov_c:
                    if city["NAME_"] in data["AREA_"][:len(prov_n)+len(city["NAME_"])]:
                        city_n = city["NAME_"]
                        city_c = city["CODE_"]

            for area in area_list:
                if area["PARENT_"] == city_c:
                    if area["NAME_"] in data["AREA_"]:
                        area_n = area["NAME_"]
                        area_c = area["CODE_"]

                    elif area["NAME_"][:2] in data["AREA_"]:
                        area_n = area["NAME_"]
                        area_c = area["CODE_"]
                        index = data["AREA_"].find(area["NAME_"][:2])
                        data["AREA_"] = data["AREA_"].replace(data["AREA_"][index:], area["NAME_"])

            if area_n is None:
                # 石家庄市桥东区被合并，数据库无匹配
                if data["AREA_"] == "河北省石家庄市桥东区":
                    area_n = "桥西区"
                    area_c = "130104"
                else:
                    for area in area_list:
                        # 省直辖县级市 todo 添加条件 area["PARENT_"][:2] == city_c[:2] and
                        if area["PARENT_"][-4:] == "9000":
                            if area["NAME_"] in data["AREA_"]:
                                city_n = "省直辖县级行政区划"
                                city_c = prov_c[:2] + "9000"
                                area_n = area["NAME_"]
                                area_c = area["CODE_"]

            if area_n is None:
                area_n = city_n
                area_c = area_c

            # 乱码跳过此次循环
            if area_n is None:
                print(data)
                continue

            # 地址清洗
            if "中国" in data["ADDR_"][:2]:
                data["ADDR_"] = data["ADDR_"].replace("中国", "")

            if data["ADDR_"] == "":
                data["ADDR_"] = data["AREA_"]

            if "电话" in data["ADDR_"]:
                index = data["ADDR_"].find("电话")
                data["ADDR_"] = data["ADDR_"].replace(data["ADDR_"][index:], "")

            if prov_n + prov_n[:-1] in data["ADDR_"]:
                data["ADDR_"] = data["ADDR_"].replace(prov_n + prov_n[:-1], prov_n)

            if prov_n not in data["ADDR_"][:len(prov_n)]:
                if prov_n[:2] in data["ADDR_"][:len(prov_n)]:
                    data["ADDR_"] = data["ADDR_"][:len(prov_n)].replace(prov_n[:2], prov_n) +  data["ADDR_"][len(prov_n):]
                else:
                    data["ADDR_"] = prov_n + data["ADDR_"]

            if city_n not in data["ADDR_"][:len(prov_n) + len(city_n)]:
                if city_n[:-1] in data["ADDR_"][:len(prov_n)+len(city_n)]:
                    data["ADDR_"] = data["ADDR_"][:len(prov_n)+len(city_n)].replace(city_n[:-1], city_n) + data["ADDR_"][len(prov_n)+len(city_n):]
                    data["ADDR_"] = data["ADDR_"].replace("市县", "市")
                else:
                    if city_c[-4:] != "9000":
                        data["ADDR_"] = data["ADDR_"][:len(prov_n)] + city_n + data["ADDR_"][len(prov_n):]
                    else:
                        if area_n in data["ADDR_"][:len(prov_n)+len(area_n)]:
                            pass
                        else:
                            if area_n in data["ADDR_"]:
                                index = data["ADDR_"].find(area_n)
                                data["ADDR_"] = prov_n + data["ADDR_"][index:]
                            else:
                                data["ADDR_"] = data["ADDR_"][:len(prov_n)].replace(prov_n, "") + data["ADDR_"][len(prov_n):]
                                data["ADDR_"] = data["ADDR_"][:len(city_n)].replace(city_n, "") + data["ADDR_"][len(city_n):]
                                index = data["ADDR_"].find(area_n) - len(area_n)
                                data["ADDR_"] = prov_n + data["AREA_"][index:] + data["ADDR_"]

                        if city_n in data["ADDR_"][:len(prov_n)+len(city_n)]:
                            data["ADDR_"] = data["ADDR_"][:len(prov_n)+len(city_n)].replace(city_n, "") + data["ADDR_"][len(prov_n)+len(city_n):]
                            # print(data["ADDR_"])

            addr_ = data["ADDR_"]

            # 定义HBase_row
            deal_time = int(float(data["DEALTIME_"]))
            row_time = 9999999999 - deal_time
            row = str(data["ENTITY_CODE_"]) + "_" + str(row_time)

            # 状态列字段
            mutation_s = Mutation(column="{}:{}".format("S", "STATUS_"), value="1")
            mutation_list.append(mutation_s)

            # 创建时间
            mutation_creat_time = Mutation(column="{}:{}".format("C", "CREATE_TIME_"), value=str(data["DATETIME_"]))
            mutation_list.append(mutation_creat_time)

            # 地区编码
            mutation_area_C = Mutation(column="{}:{}".format("C", "AREA_CODE_"), value=area_c)
            mutation_list.append(mutation_area_C)

            # 学校名称
            mutation_name = Mutation(column="{}:{}".format("F", "NAME_"), value=str(data["NAME_"]))
            mutation_list.append(mutation_name)

            # 学校图片
            mutation_url = Mutation(column="{}:{}".format("F", "IMAGES_"), value=str(data["IMAGES_"]))
            mutation_list.append(mutation_url)

            # 学校级别(是否公办)
            mutation_url = Mutation(column="{}:{}".format("F", "GRADE_"), value=str(data["GRADE_"]))
            mutation_list.append(mutation_url)

            # 省级编码
            mutation_p_c = Mutation(column="{}:{}".format("F", "PROVINCE_CODE_"), value=prov_c)
            mutation_list.append(mutation_p_c)

            # 省级名称
            mutation_p_n = Mutation(column="{}:{}".format("F", "PROVINCE_NAME_"), value=prov_n)
            mutation_list.append(mutation_p_n)

            # 市级编码
            mutation_c_c = Mutation(column="{}:{}".format("F", "CITY_CODE_"), value=city_c)
            mutation_list.append(mutation_c_c)

            # 市级名称
            mutation_c_n = Mutation(column="{}:{}".format("F", "CITY_NAME_"), value=city_n)
            mutation_list.append(mutation_c_n)

            # 区县编码
            mutation_area_c = Mutation(column="{}:{}".format("F", "DISTRICT_CODE_"), value=area_c)
            mutation_list.append(mutation_area_c)

            # 区县名称
            mutation_area_n = Mutation(column="{}:{}".format("F", "DISTRICT_NAME_"), value=area_n)
            mutation_list.append(mutation_area_n)

            # 学校性质(公办民办私立)
            mutation_addr = Mutation(column="{}:{}".format("F", "SCHOOL_TYPE_"), value=str(data["SCHOOL_TYPE_"]))
            mutation_list.append(mutation_addr)

            # 学校等级
            mutation_addr = Mutation(column="{}:{}".format("F", "PERIOD_"), value=str(data["PERIOD_"]))
            mutation_list.append(mutation_addr)

            # 学校电话
            mutation_addr = Mutation(column="{}:{}".format("F", "TEL_"), value=str(data["TEL_"]))
            mutation_list.append(mutation_addr)

            # 学校网站
            mutation_addr = Mutation(column="{}:{}".format("F", "WEBSITE_"), value=str(data["WEBSITE_"]))
            mutation_list.append(mutation_addr)

            # 学校地址
            mutation_addr = Mutation(column="{}:{}".format("F", "ADDR_"), value=addr_)
            mutation_list.append(mutation_addr)

            # 学校简介
            mutation_addr = Mutation(column="{}:{}".format("F", "BRIEF_"), value=str(data["BRIEF_"]))
            mutation_list.append(mutation_addr)

            # 页面地址
            mutation_addr = Mutation(column="{}:{}".format("F", "URL_"), value=str(data["URL_"]))
            mutation_list.append(mutation_addr)

            # 处理时间
            mutation_addr = Mutation(column="{}:{}".format("F", "DEALTIME_"), value=str(data["DEALTIME_"]))
            mutation_list.append(mutation_addr)

            # 实体名称
            mutation_addr = Mutation(column="{}:{}".format("F", "ENTITY_NAME_"), value=str(data["ENTITY_NAME_"]))
            mutation_list.append(mutation_addr)

            # 实体编码
            mutation_addr = Mutation(column="{}:{}".format("F", "ENTITY_CODE_"), value=str(data["ENTITY_CODE_"]))
            mutation_list.append(mutation_addr)

            # MongoDB_id
            mutation_id = Mutation(column="{}:{}".format("F", "_id"), value=str(data["_id"]))
            mutation_list.append(mutation_id)

            batch_mutation = BatchMutation(row, mutation_list)
            batch_list.append(batch_mutation)

        return batch_list

    def data_to_hbase(self, entity_type, batch_list):
        hbase_client = HbaseClient(hbase_table=entity_type)
        client, transport = hbase_client.client_to_hbase()
        hbase_client.check_table_exists(client, transport=transport)
        hbase_client.thread_insert_data(client, transport=transport, batch_list=batch_list)
        hbase_client.client_close(transport)

    def run(self):
        script = GenericScript(entity_code="SXUESCHOOL", entity_type="SCHOOL_FINASSIST")
        mongo_data_list = script.data_from_mongo()
        province_list, city_list, area_list, dir_area_list = script.area_from_mysql()
        batch_list = self.data_shuffle(mongo_data_list, province_list, city_list, area_list)
        # self.data_to_hbase(entity_type="SCHOOL_FINASSIST", batch_list=batch_list)


if __name__ == '__main__':
    script_school = SchoolFinassistScript()
    script_school.run()
