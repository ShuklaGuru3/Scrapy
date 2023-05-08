import scrapy
import csv
import sqlite3

class NoonSpider(scrapy.Spider):
    name = "noon_spider"
    
    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'ROTATING_PROXY_LIST_PATH': 'proxies.txt'
    }
    
    def start_requests(self):
        url = "https://www.noon.com/egypt-en/sports-and-outdoors/exercise-and-fitness/yoga-16328/"
        yield scrapy.Request(url=url, callback=self.parse)
        
    def parse(self, response):
        products = response.xpath('//div[@class="productContainer"]')
        for product in products:
            yield {
                'product_name': product.xpath('.//div[@class="name"]/text()')
                                 .get().strip(),
                'product_brand': product.xpath('.//div[@class="brandName"]/text()')
                                  .get().strip(),
                'product_price': product.xpath('.//div[@class="priceWrapper"]/text()')
                                  .get().strip(),
                'product_seller': product.xpath('.//div[@class="sellerName"]/text()')
                                   .get().strip(),
                'product_url': response.urljoin(product.xpath('.//a/@href')
                                               .get().strip())
            }
            
        next_page = response.xpath('//a[@class="nextBtn"]/@href')
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page.get()),
                                 callback=self.parse)
            
    def closed(self, reason):
        # write data to CSV file
        with open('products.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Product Name', 'Brand', 'Price', 'Seller', 'URL'])
            for item in self.exported_items:
                writer.writerow([item['product_name'], item['product_brand'], item['product_price'], item['product_seller'], item['product_url']])
                
        # write data to SQLite3 database
        conn = sqlite3.connect('products.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS products
                     (product_name TEXT, product_brand TEXT, product_price TEXT, product_seller TEXT, product_url TEXT)''')
        for item in self.exported_items:
            c.execute("INSERT INTO products VALUES (?, ?, ?, ?, ?)",
                      (item['product_name'], item['product_brand'], item['product_price'], item['product_seller'], item['product_url']))
        conn.commit()
        conn.close()

