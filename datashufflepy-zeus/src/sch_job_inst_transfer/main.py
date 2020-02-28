# -*- coding: utf-8 -*-
import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath[:-22])
# print(curPath[:-22])
# quit()

from log.data_log import Logger
from database._phoenix_hbase import PhoenixHbase, value_replace
from database._mysql import MysqlClient


class DataTransfer:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super(DataTransfer, cls).__new__(cls)
        config = {
            # "host": "192.168.1.103",
            "host": "172.22.69.43",
            "port": 3306,
            "table": "sch_job_inst",
            "database": "ijep",
            "user": "ijep",
            "password": "ijep#O2018",
            "charset": "utf8"
        }
        cls.mysql_client = MysqlClient(**config)
        cls.mysql_connection = cls.mysql_client.client_to_mysql()
        cls.hbase_client = PhoenixHbase("SCH_JOB_INST")
        cls.hbase_connection = cls.hbase_client.connect_to_phoenix()
        cls.logger = Logger().logger
        return cls.instance

    def __init__(self, days):
        self.days = days
        self.count = 0

    def get_data_from_mysql(self):
        where_condition = f"WHERE DATE_SUB(CURDATE(), INTERVAL {self.days} DAY) >= DATE (MODIFIED_TIME_) limit 0,200"
        result = self.mysql_client.search_from_mysql(connection=self.mysql_connection, where_condition=where_condition)

        return result

    def main(self):
        result_list = self.get_data_from_mysql()
        list_for_id = tuple(i["ID_"] for i in result_list)
        self.mysql_client.delete_from_mysql(connection=self.mysql_connection, where_condition=f"WHERE ID_ in {list_for_id}")
        try:
            self.hbase_client.upsert_to_phoenix(connection=self.hbase_connection, data_list=result_list)
        except:
            pass
        # if result_list:
        #     for i in result_list:
        #         # list_for_id.append(i["ID_"])
        #
        #         delete_count = self.mysql_client.delete_from_mysql(connection=self.mysql_connection,
        #                                                            where_condition=f"WHERE ID_ = \'{i['ID_']}\'")
        #         if delete_count:
        #             self.count += delete_count
        #             try:
        #                 self.hbase_client.upsert_to_phoenix_by_one(connection=self.hbase_connection, data=i)
        #             except:
        #                 self.mysql_client.insert_to_mysql(connection=self.mysql_connection, data=i)
        #     self.logger.info(f"MySQL 删除共 {self.count} 条")
        #     self.logger.info(f"HBase 插入共 {self.hbase_client.count} 条")

        # else:
        #     self.logger.info("查取数据条数为 0, 程序即将退出.")
        #     self.mysql_connection.close()
        #     self.hbase_connection.close()


if __name__ == '__main__':
    try:
        days_ago = sys.argv[1]
    except:
        days_ago = "3"

    script = DataTransfer(days=days_ago)
    print("启动成功")
    script.main()

    # 建表语句
    """
    CREATE TABLE "SCH_JOB_INST" ("ID_" varchar primary key,
                                 "C"."JOB_CODE_" VARCHAR,
                                 "C"."JOB_NAME_" VARCHAR,
                                 "C"."GROUP_CODE_" VARCHAR,
                                 "C"."PARAM_" VARCHAR,
                                 "C"."SERVICE_ID_" VARCHAR,
                                 "C"."START_TIME" VARCHAR,
                                 "C"."END_TIME" VARCHAR,
                                 "C"."EXEC_TIME_" VARCHAR,
                                 "C"."STATUS_" VARCHAR,
                                 "C"."TIMEOUT_TIMES_" VARCHAR,
                                 "C"."ERROR_TYPE_" VARCHAR,
                                 "C"."ERROR_TIMES_" VARCHAR,
                                 "C"."ERROR_MESSAGE_" VARCHAR,
                                 "C"."RECOVERED_" VARCHAR,
                                 "C"."TRIGGER_KEY_" VARCHAR,
                                 "C"."LAST_TRIGGER_KEY_" VARCHAR,
                                 "C"."SCHEDULED_FIRE_TIME_" VARCHAR,
                                 "C"."ORIGIN_START_TIME_" VARCHAR,
                                 "C"."SERVER_INSTANCE_ID_" VARCHAR,
                                 "C"."TENANT_ID_" VARCHAR,
                                 "C"."CREATED_BY_ID_" VARCHAR,
                                 "C"."CREATED_BY_NAME_" VARCHAR,
                                 "C"."CREATED_TIME_" VARCHAR,
                                 "C"."DELFLAG_" VARCHAR,
                                 "C"."DISPLAY_ORDER_" VARCHAR,
                                 "C"."MODIFIED_BY_ID_" VARCHAR,
                                 "C"."MODIFIED_BY_NAME_" VARCHAR,
                                 "C"."MODIFIED_TIME_" VARCHAR,
                                 "C"."VERSION_" VARCHAR,
                                 "C"."CLIENT_INSTANCE_ID_" VARCHAR
                                )IMMUTABLE_ROWS = true
    """
