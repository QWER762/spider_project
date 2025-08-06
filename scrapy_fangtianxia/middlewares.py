# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy import Request
from scrapy import signals
import scrapy
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import time
import random

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class ScrapyFangtianxiaSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ScrapyFangtianxiaDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

class SliderCaptchaMiddleware:
    def __init__(self):
        # 初始化浏览器（无头模式，避免弹窗）
        service = Service(r'D:\chromedriver-win64\chromedriver-win64\chromedriver.exe')
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 无头模式
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(options=chrome_options,service=service)

    def process_response(self, request, response, spider):
        # 判断是否是滑块验证页面（通过 URL 或内容）
        if "check.3g.fang.com" in response.url or "拖动滑块验证" in response.text:
            try:
                # 1. 注入爬虫的 Cookie（保持会话一致）
                self.driver.delete_all_cookies()  # 清空原有 Cookie
                for k, v in request.cookies.items():
                    self.driver.add_cookie({
                        "name": k,
                        "value": v,
                        "domain": ".fang.com"  # 注意域名，需覆盖子域名
                    })

                # 2. 重新加载验证页面（携带爬虫的 Cookie）
                self.driver.get(response.url)
                time.sleep(3)  # 等待页面加载

                # 3. 定位滑块和容器（需根据实际页面调整选择器！）
                # 示例选择器（需替换为真实页面的 CSS 选择器）：
                slider = self.driver.find_element(By.CSS_SELECTOR, ".handler.handler_bg")  # 滑块
                container = self.driver.find_element(By.CSS_SELECTOR,".drag_text")  # 滑块容器

                # 4. 模拟滑块拖动（带随机轨迹，避免被识别为机器）
                action = ActionChains(self.driver)
                action.click_and_hold(slider).perform()  # 按住滑块

                # 计算拖动距离（示例：假设容器宽度为缺口位置，实际需更精确计算！）
                # 真实场景需通过图片对比找缺口，此处简化为容器宽度的 80%
                container_width = int(container.value_of_css_property("width").replace("px", ""))  # 300
                distance = container_width  # 需拖动300px（从left:0到left:300）

                # 模拟人类拖动：分段随机移动
                action.click_and_hold(slider).perform()  # 按住滑块
                action.move_by_offset(distance, 0).perform()  # 一次性移动到目标位置
                time.sleep(random.uniform(0.05, 0.1))  # 拖动后短暂停顿再释放，更自然


                action.release().perform()  # 释放滑块
                time.sleep(3)  # 等待验证结果（页面跳转或提示）

                WebDriverWait(self.driver, 10).until(
                    lambda d: "check.3g.fang.com" not in d.current_url
                )

                new_cookies = self.driver.get_cookies()
                cookie_dict = {cookie["name"]: cookie["value"] for cookie in new_cookies}

                # 更新请求的Cookie，确保后续请求携带验证状态
                request.cookies.update(cookie_dict)
                # 5. 将验证后的页面转为 Scrapy 响应
                new_body = self.driver.page_source.encode("utf-8")
                new_response = HtmlResponse(
                    url=self.driver.current_url,
                    body=new_body,
                    encoding="utf-8",
                    request=request
                )
                return new_response

            except Exception as e:
                spider.logger.error(f"滑块验证失败: {e}")
                # 验证失败，可选择重试（抛出异常让 Scrapy 重试）
                raise e
            finally:
                self.driver.delete_all_cookies()  # 清理 Cookie，避免干扰后续请求

        return response


    def close(self, spider):
        # 关闭浏览器（爬虫关闭时触发）
        self.driver.quit()
