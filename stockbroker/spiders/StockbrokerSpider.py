import scrapy
import pymongo
import logging
import datetime
from stockbroker.items import StockbrokerItem

# for execute: scrapy craw stockbroker_spider

class StockbrokerSpider(scrapy.Spider):
    months_id = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'des']
    name = "stockbroker_spider"
    start_urls = ["https://br.financas.yahoo.com/quote/ITUB4.SA/history?p=ITUB4.SA"]
    symbol = "5d50d50026537d402d0203e4"

    def start_requests(self):
        self.generate_url()
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
            )

    def parse(self, response):
        count = 0
        item = StockbrokerItem()
        for td in response.css("td"):
            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Ta(start) Pend(10px)' and count == 0):
                item['date'] = self.convert_date(td.css('span::text').extract_first())

            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 1):
                item['open'] = self.convert(td.css('span::text').extract_first())

            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 2):
                item['high'] = self.convert(td.css('span::text').extract_first())

            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 3):
                item['low'] = self.convert(td.css('span::text').extract_first())
            
            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 4):
                item['close'] = self.convert(td.css('span::text').extract_first())

            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 5):
                item['close_ajusted'] = self.convert(td.css('span::text').extract_first())

            if (td.css('td::attr(class)').extract_first() == 'Py(10px) Pstart(10px)' and count == 6):
                item['volume'] = td.css('span::text').extract_first()
            
            if (td.css('td::attr(class)').extract_first() == 'Ta(c) Py(10px) Pstart(10px)'):
                count = 0
            else:
                count += 1

            if (count == 7):
                item['symbol'] = self.symbol
                count = 0
                yield item
                item = StockbrokerItem()

    def generate_url(self):
        client = pymongo.MongoClient("mongodb://teste:teste@cluster0-shard-00-00-jfdue.mongodb.net:27017,cluster0-shard-00-01-jfdue.mongodb.net:27017,cluster0-shard-00-02-jfdue.mongodb.net:27017/mercado?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority")
        db = client["mercado"]
        collection = db["symbols"]

        for symbol in collection.find():
            logging.debug(symbol)
            # print(x)
    
    def convert(self, value):
        return float(value.replace(',','.'))

    def convert_date(self, date):
        day, d1, month_name, d2, year = date.split(' ')
        month = self.months_id[self.months.index(month_name)]
        return datetime.date(int(year), int(month), int(day)).strftime("%Y-%m-%d")
