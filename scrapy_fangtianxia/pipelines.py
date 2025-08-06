# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
from itemadapter import ItemAdapter
from scrapy_fangtianxia.items import NewHouseItem, ESFHouseItem


class ScrapyFangtianxiaPipeline:
    def __init__(self):
        # 新房CSV
        self.nh_file = open('newhouse.csv', 'w', encoding='utf-8-sig', newline='')
        self.nh_writer = csv.DictWriter(self.nh_file, fieldnames=NewHouseItem.fields.keys())
        self.nh_writer.writeheader()

        # 二手房CSV
        self.esf_file = open('esf.csv', 'w', encoding='utf-8-sig', newline='')
        self.esf_writer = csv.DictWriter(self.esf_file, fieldnames=ESFHouseItem.fields.keys())
        self.esf_writer.writeheader()

    def process_item(self, item, spider):
        if isinstance(item, NewHouseItem):
            self.nh_writer.writerow(ItemAdapter(item).asdict())
        elif isinstance(item, ESFHouseItem):
            self.esf_writer.writerow(ItemAdapter(item).asdict())
        return item

    def close_spider(self, spider):
        self.nh_file.close()
        self.esf_file.close()

    # def __init__(self):
    #     self.newhouse_fp = open('newhouse.json','w',encoding='utf-8')
    #     self.esf_fp = open('esf.json', 'w', encoding='utf-8')
    #
    # def process_item(self, item, spider):
    #     self.newhouse_fp.write(str(item))
    #     self.esf_fp.write(str(item))
    #     return item
    #
    # def close_spider(self,spider):
    #     self.newhouse_fp.close()
    #     self.esf_fp.close()
