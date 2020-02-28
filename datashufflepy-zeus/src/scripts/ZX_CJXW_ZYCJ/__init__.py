# -*- coding: utf-8 -*-
import re

from scripts import GenericScript


class BranchNews(GenericScript):
    def generic_shuffle(self, data):
        """
        清洗规则写这里
        :param data:
        :return:
        """
        # different shuffle rule
        data["CONTENT_"] = re.sub(r"(var.*?;\|)(?![a-zA-Z])", "", data["CONTENT_"])
        re_data = super(BranchNews, self).generic_shuffle(data=data)
        return re_data


if __name__ == '__main__':
    script = BranchNews(table_name="CHA_BRANCH_NEWS", collection_name="ZX_CJXW_ZYCJ")
    script.main()
