
import scrapy
import re

from scrapy import Request

from scrapy_fangtianxia.items import NewHouseItem
from scrapy_fangtianxia.items import ESFHouseItem
from scrapy_redis.spiders import RedisSpider
# scrapy.Spider
class FangSpider(RedisSpider):
    name = "fang"
    allowed_domains = [
        "www.fang.com",
        "fang.com",  # 添加主域名  这里因为域名报错了
        "newhouse.fang.com",
        "esf.fang.com"
    ]
    # start_urls = ["https://www.fang.com/SoufunFamily.htm"]
    redis_key = 'fang:start_urls'

    def start_requests(self):
        url = "https://www.fang.com/SoufunFamily.htm"
        cookie_str = '你可以填写的cookie'
        cookie_dict = {}
        for item in cookie_str.split(';'):
            item = item.strip()  # 去除前后空格
            if item:  # 避免空字符串
                k, v = item.split("=", 1)  # 只按第一个 "=" 分割（防止值含 "="）
                cookie_dict[k] = v
        yield scrapy.Request(
            url=url,
            cookies=cookie_dict,  # 或直接传字符串：cookies=cookies
            callback=self.parse
        )

    def parse(self, response,**kwargs):
        trs = response.xpath('//div[@class="outCont"]//tr')
        province = None
        for tr in trs:
            tds = tr.xpath('.//td[not(@class)]')
            province_td = tds[0]
            province_text = province_td.xpath('.//strong//text()').get(default='')
            province_text = re.sub(r'\s', '', province_text)
            if province_text:
                province = province_text
                if province == '其它':
                    continue
            city_td = tds[1]
            city_links = city_td.xpath('.//a')
            for city_link in city_links:
                city = city_link.xpath('.//text()').extract_first()
                city_url = city_link.xpath('.//@href').extract_first()
                # 构建新房的链接
                url_module = city_url.split('.',1)
                scheme = url_module[0]
                domain = url_module[1]
                if '/' in domain:
                    newhouse_url = scheme + '.newhouse.' + domain + 'house/s/'
                            # 构建二手房的链接
                    esf_url = scheme + '.esf.' + domain
                else:
                    newhouse_url = scheme + '.newhouse.' + domain + '/' +'house/s/'
                    # 构建二手房的链接
                    esf_url = scheme + '.esf.' + domain + '/'

                yield scrapy.Request(url=newhouse_url, callback=self.parse_newhouse,
                                     meta={'info':(province, city)})

                yield scrapy.Request(url=esf_url, callback=self.parse_esf,
                                     meta={'info':(province, city)})


    def parse_newhouse(self,response):
        province, city = response.meta.get('info')
        lis = response.xpath('//div[contains(@class, "nl_con clearfix")]//ul//li')
        for li in lis:
            name = li.xpath('.//div[@class="nlcd_name"]/a/text()').get().strip()
            rooms = li.xpath('.//div[contains(@class,"house_type clearfix")]//a//text()').getall()
            areas = li.xpath('.//div[contains(@class,"house_type clearfix")]//text()').getall()[-1]
            area = re.sub(r'—','',areas).strip()
            districts = "".join(li.xpath('.//div[@class="address"]/a//text()').getall())
            address = li.xpath('.//div[@class="address"]/a/@title').get()
            district = re.search(r'.*\[(.+)\].*', districts).group(1)
            sale = li.xpath('.//div[@class="fangyuan"]/span/text()').get()
            price = "".join(li.xpath('.//div[@class="nhouse_price"]//text()').getall())
            origin_url = li.xpath('.//div[@class="nlcd_name"]/a/@href').get()

            item = NewHouseItem(name=name, rooms=rooms, area=area, district=district, address=address,
                                 sale=sale, price=price, origin_url=origin_url, province=province,
                                 city=city)
            yield item

        next_url = response.xpath('//div[@class="page"]//a[@class="next"]/@href').get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse,
                                 meta={'info':(province,city)})

    def parse_esf(self,response):
        province, city = response.meta.get('info')
        lists = response.xpath('//div[contains(@class, "shop_list shop_list_4")]//dl')
        for lis in lists:
            item = ESFHouseItem(province=province, city=city)
            name = lis.xpath('.//p[@class="add_shop"]/a/text()').get()
            address = lis.xpath('.//p[@class="add_shop"]/span/text()').get()
            infos = lis.xpath('.//p[@class="tel_shop"]//text()').getall()
            for info in infos:
                if '室' in info:
                    item['rooms'] = info
                elif '层' in info:
                    item['floor'] = info
                elif '向' in info:
                    item['toward'] = info
                elif '年建' in info:
                    item['year'] = info
                elif '㎡' in info:
                    item['area'] = info
            prices = lis.xpath('.//dd[@class="price_right"]//text()').getall()
            price_1 = prices[0]
            price = price_1 + '万'
            unit_price = prices[-1]
            origin = lis.xpath('.//h4[@class="clearfix"]/a/@href').get()
            origin_url = response.urljoin(origin)
            item['name'] = name
            item['address'] = address
            item['price'] = price
            item['unit_price'] = unit_price
            item['origin_url'] = origin_url
            yield item
        next_url = response.xpath('//div[@class="page_box"]//p[1]//a/@href').get()
        if next_url:
            # next_url = response.urljoin(next_url)
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf,
                                 meta={'info': (province, city)})
