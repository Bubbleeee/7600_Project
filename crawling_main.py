import pandas as pd
import requests
import re
from collections import OrderedDict
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
import unicodedata
import time
import csv
import codecs
import eventlet
import os


class StockArticle(object):
    def __init__(self, url):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'}
        self.url = url
        self.date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        # url保存的完整路径为：url_path + 当日日期 + url_filename
        self.url_path = "/Users/liusiyu02/Desktop/DASC7600/Data/"
        self.url_filename = "_urls.csv"
        self.article_path = "/Users/liusiyu02/Desktop/DASC7600/Article/"
        self.article_filename = "_articles.csv"

    def request_url(self, url):
        try:
            response = requests.get(url=url, headers=self.headers)
            response.raise_for_status()
            # 指定encoding类型
            response.encoding = response.apparent_encoding
            r = response.text
            return r
        except Exception as err:
            print(err)
            return ""
            # pass

    def get_url(self):

        # 存储已访问的url
        visited = set()
        # 存储已获取的url
        queue = OrderedDict([(self.url, 0)])
        cnt = 1

        url, layer = queue.popitem(last=False)

        while layer < 3:
            visited |= {url}

            print('已经抓取: ' + str(cnt) + '   正在抓取 <---  ' + url)
            print('visited内已有' + str(len(visited)) + '个元素')
            print('队列现有' + str(len(queue)) + '个元素')
            print('正在第' + str(layer) + '层')
            cnt += 1

            if ".PDF" not in url:
                r = self.request_url(url)
                if r == "":
                    print("有问题！！！")
                    pass
                # 从url中提取数据
                soup = BeautifulSoup(r, 'lxml')
                try:
                    url_list = soup.find_all("a", href=re.compile("finance.sina.com.cn"))
                    for item in url_list:
                        t = item.get('href')
                        if t not in queue and t not in visited:
                            queue[t] = layer + 1
                            print('加入队列 --->  ' + t + '  位于第' + str(queue[t]) + '层')
                except Exception as err:
                    print(err)
                    pass
            url, layer = queue.popitem(last=False)

        return visited

    def write_url(self, root_finance):
        # def write_url(self, root_finance, root_cj):
        # 指定保存路径
        path = self.url_path + str(self.date) + self.url_filename

        # 写入url
        with codecs.open(path, 'w', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for item in root_finance:
                writer.writerow([item])

        return path

    @staticmethod
    def read_url(path):
        url_df = pd.read_csv(path, header=None)
        url_df.columns = ['URL']
        return url_df

    def get_stock_article(self):
        url_path = self.url_path+str(self.date)+self.url_filename
        if os.path.exists(url_path):
            url_df = self.read_url(url_path)
        else:
            root_finance = self.get_url()
            url_path = self.write_url(root_finance)
            url_df = self.read_url(url_path)

        # 爬取的数据格式：文章发表时间、标题、内容
        article_info = pd.DataFrame(columns=['URL', 'Stock_ID', 'Stock_Name', 'Publish Time', 'Title', 'Article'])

        for index, row in url_df.iterrows():
            r = self.request_url(row['URL'])
            if r == "":
                print("有问题！！！")
                pass
            soup = BeautifulSoup(r, 'lxml')
            try:
                # if 该url的网页类型为news
                if soup.find_all("meta", attrs={"property": "og:type", "content": "news"}):
                    # re_article = "<div class="article" id="artibody"><p>(.*?)</p>"
                    title = soup.find("h1", class_="main-title").text
                    pub_time = soup.find("span", class_="date").text
                    stock_list = soup.find_all("span", id=re.compile("stock_"))
                    p_list = soup.find("div", id="artibody").find_all('p', class_=False)
                    article = []
                    stock_id = str()
                    stock_name = str()
                    for i in range(len(p_list)):
                        # 过滤前端代码及多余空格
                        p = unicodedata.normalize('NFKC', p_list[i].text.replace(" ", ""))
                        article.append(p)
                    cnt = 0
                    for item in stock_list:
                        stock_id = item.attrs['id']
                        stock_name = item.text
                    if len(stock_list) == 1:
                        cnt += 1
                        print("共有" + str(cnt) + "个新闻只出现了1只股票")

                        df = pd.DataFrame([[row['URL'], stock_id, stock_name, pub_time, title, article]],
                                          columns=['URL', 'Stock_ID', 'Stock_Name', 'Publish Time', 'Title', 'Article'])
                        article_info = article_info.append(df, ignore_index=True)
                    # 用于review哪些url的内容存了哪些没存
                    print(index)
                else:
                    print('Filtered: ' + str(index))
            # if 有错，则直接跳过当前url，进入下一循环
            except Exception as err:
                print(err)
                pass
            continue

        path = self.article_path + str(self.date) + self.article_filename
        try:
            article_info.to_csv(path, encoding='utf-8-sig')
            print("Saved")
        except Exception as err:
            print(err)


if __name__ == '__main__':
    t1 = time.time()
    url = "https://finance.sina.com.cn/stock/"
    crawl = StockArticle(url)
    crawl.get_stock_article()
    t2 = time.time()
    print('共耗时：' + str(int((t2 - t1) / 60)) + '分钟')
