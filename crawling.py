import pandas as pd
import requests
import re
from collections import OrderedDict
from bs4 import BeautifulSoup
import unicodedata
import time
import csv
import codecs
import os
import jieba


class CrawlSina(object):
    def __init__(self, url):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'}
        self.url = url
        self.date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        # e.g., url保存的完整路径为：url_path + 当日日期 + "_urls.csv"
        self.url_path = "data/sina/urls/"
        self.article_path = "data/sina/articles/"

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
        path = self.url_path + str(self.date) + "_urls.csv"

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

    # 生成情感词汇集S
    def get_jiebalist(self, x):
        # 对字符串'['string1','string1']'结巴分词¶，得到list：[[string1 word],[string2 word]]
        result = []
        s = ''.join(str(e) for e in x)
        y = s.strip("[").strip("]").split(", ")
        for string in y:
            tmp = " ".join(jieba.cut(string)).split()
            result.append(tmp)
        return result

    def trans_lists2list(self, x):
        # 将包含多个lists的list x合并为1个list(即一篇文章的词汇存在1个list中)
        return [i for p in x for i in p]

    def get_stock_article(self):
        url_path = self.url_path + str(self.date) + "_urls.csv"
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
                    for item in stock_list:
                        stock_id = item.attrs['id']
                        stock_name = item.text
                    if len(stock_list) == 1:
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

        article_info['doc_Jieba_List'] = article_info.Article.apply(self.get_jiebalist)
        article_info['doc_Jieba'] = article_info.doc_Jieba_List.apply(self.trans_lists2list)
        article_info.drop(columns=['doc_Jieba_List'], inplace=True)
        print("----Finish jieba----")

        path = self.article_path + str(self.date) + "_articles.csv"
        try:
            article_info.to_csv(path, encoding='utf-8-sig')
            print("Saved")
            return article_info
        except Exception as err:
            print(err)


class CrawlStarquote(object):

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'}

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

    @staticmethod
    def get_stock_descrption(soup):
        df = pd.DataFrame(columns=['ID', 'Stock', 'Description'])
        tbody = soup.find("tbody", id='t_body')
        tr = tbody.find_all("tr")
        for item in tr:
            children = list(item.children)
            stock_id = children[1].text
            stock = children[2].text
            description = children[3].text
            df = df.append({'ID': stock_id, 'Stock': stock, 'Description': description}, ignore_index=True)

        return df

if __name__ == '__main__':

    # Crawling news from sina
    tstart_sina = time.time()
    url_sina = "https://finance.sina.com.cn/stock/"
    crawl_sina = CrawlSina(url_sina)
    sina_news = crawl_sina.get_stock_article()
    tend_sina = time.time()
    print('共耗时：' + str(int((tend_sina - tstart_sina) / 60)) + '分钟')

    # Crawling news from stockstar
    tstart_starquote = time.time()
    df_starquote = pd.DataFrame(columns=['ID', 'Stock', 'Description'])
    for page in range(1, 139):
        url_starquote = "https://quote.stockstar.com/Comment/1_0_" + str(page) + ".html"
        crawl_starquote = CrawlStarquote()
        r = crawl_starquote.request_url(url_starquote)
        if r == "":
            print("有问题！！！")
            continue
        soup = BeautifulSoup(r, 'html.parser')
        df = crawl_starquote.get_stock_descrption(soup)
        df_starquote = df_starquote.append(df)
        print("第" + str(page) + "页已抓取完毕")
    date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    starquote_path = "data/starquote/" + str(date) + "_starquote_news.csv"
    df_starquote.to_csv(starquote_path, encoding='utf-8-sig')
    tend_starquote = time.time()
    print('共耗时：' + str(int((tend_starquote - tstart_starquote) / 60)) + '分钟')
