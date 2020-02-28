# -*- coding: utf-8 -*-
# 编写HBase插入语句
# def make_batch_list(self, data_list):
#     batch_list = list()
#     self.logger.info("正在编写HBase插入语句")
#
#     for data in data_list:
#         # 定义HBase_row
#         row_time = 9999999999 - int(data["DEALTIME_"])
#         row = str(row_time) + "_" + str(data["ENTITY_CODE_"])
#         mutation_list = []
#
#         for key, value in data.items():
#             # 向HBase插入数据
#             mutation = Mutation(column="{}:{}".format(self.hbase_colum_family, str(key)), value=str(value))
#             mutation_list.append(mutation)
#         batch_mutation = BatchMutation(row, mutation_list)
#         batch_list.append(batch_mutation)
#     # print(batch_list)
#     # return batch_list

