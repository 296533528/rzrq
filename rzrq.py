from selenium import webdriver
import psycopg2
import time
import datetime
from lxml import etree
import redis
import fire

redis_conn = redis.Redis(host='redis', port=6379,
                         decode_responses=True)  # host是redis主机，需要redis服务端和客户端都启动 redis默认端口是6379
conn = psycopg2.connect(database="east", user="scraper", password="68368", host="db", port="5432")
cur = conn.cursor()



def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
       CREATE TABLE IF NOT EXISTS rzrq(
                date DATE PRIMARY KEY,           /* 命名从 api get_rzrq_ggmx  */
                seq INT,                         /* 序号 */
                sym VARCHAR (50),                /* 证券代码， 上海 .SH  深圳 .SZ */
                symname VARCHAR(20),             /* 证券简称 */
                close NUMERIC (6, 2),           /* 收盘价，元 */
                yield NUMERIC (8, 4),           /* 收盘价涨跌幅  0.0166   for 1.66% */
                rzye NUMERIC (20, 2),           /* 融资余额， 元  */
                rzyezb NUMERIC (8, 4),   /* 融资余额占流通市值比 0.0071 for 0.71%  */
                rzmre NUMERIC (20, 2),              /* 融资买入额， 元  */
                rzche NUMERIC (20, 2),              /* 融资偿还额， 元 */
                rzjme NUMERIC (20, 2),              /* 融资净买入， 元 */
                rqye NUMERIC (20, 2),                  /* 融券余额， 股 */
                rqyl NUMERIC (20),                  /* 融券余量， 股 */
                rqmcl NUMERIC (20),                 /* 融券卖出量， 股 */
                rqchl NUMERIC (20),                 /* 融券偿还量， 股  */
                rqjmc NUMERIC (20),                 /* 融券净卖出， 股 */
                rzrqye NUMERIC (20, 2),             /* 融资融券余额，元 */
                rzrqce NUMERIC (20, 2)              /* 融资融券差额，元 */
            );
            CREATE INDEX ON rzrq (date);
            CREATE INDEX ON rzrq (sym);
            """)


    # conn.commit()
    print("table rzrq created/exists")


def get_driver():
    create_table(conn)
    driver = webdriver.Remote(command_executor='http://zalenium:4445/wd/hub',
                              desired_capabilities={'browserName': 'chrome'})
    return driver


def create_assist_date(datestart=None, dateend=None):  # 创建日期列表
    if datestart is None:
        datestart = '20110101'
    if dateend is None:
        dateend = datetime.datetime.now().strftime('%Y%m%d')
    datestart = datetime.datetime.strptime(datestart, '%Y%m%d')
    dateend = datetime.datetime.strptime(dateend, '%Y%m%d')
    date_list = []
    date_list.append(datestart.strftime('%Y-%m-%d'))
    while datestart < dateend:
        datestart += datetime.timedelta(days=+1)
        date_list.append(datestart.strftime('%Y-%m-%d'))
    print(date_list)
    for i in date_list:
        redis_conn.lpush('date', i)
    return date_list


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return yesterday


def parse(jx, flag,date):
    tbody = jx.xpath('//*[@id="rzrqjymxTable"]/tbody/tr')
    for tr in tbody:
        try:
            num = tr.xpath('td/text()')[0]
        except:
            num = '-'
        try:
            code = tr.xpath('td[2]/a/text()')[0]
        except:
            code = '-'
        try:
            name = tr.xpath('td[3]/a/text()')[0]
        except:
            name = '-'
        try:
            closing_price = tr.xpath('td[4]/span/text()')[0]
        except:
            closing_price = '-'
        try:
            up_down = tr.xpath('td[5]/span/text()')[0]
        except:
            up_down = '-'
        try:
            balance = tr.xpath('td[7]/text()')[0]
        except:
            balance = '-'
        try:
            proportion = tr.xpath('td[8]/text()')[0]
        except:
            proportion = '-'
        try:
            purchase = tr.xpath('td[9]/text()')[0]
        except:
            purchase = '-'
        try:
            sales = tr.xpath('td[10]/text()')[0]
        except:
            sales = '-'
        try:
            chazhi = tr.xpath('td[11]/span/text()')[0]
        except:
            chazhi = '-'
        try:
            rq_balance = tr.xpath('td[12]/text()')[0]
        except:
            rq_balance = '-'
        try:
            rq_quantity = tr.xpath('td[13]/text()')[0]
        except:
            rq_quantity = '-'
        try:
            rq_maichu = tr.xpath('td[14]/text()')[0]
        except:
            rq_maichu = '-'
        try:
            rq_changhuan = tr.xpath('td[15]/text()')[0]
        except:
            rq_changhuan = '-'
        try:
            rq_chazhi = tr.xpath('td[16]/span/text()')[0]
        except:
            rq_chazhi = '-'
        try:
            rzrq_balance = tr.xpath('td[17]/text()')[0]
        except:
            rzrq_balance = '-'
        try:
            rzrq_balance_chazhi = tr.xpath('td[18]/text()')[0]
        except:
            rzrq_balance_chazhi = '-'
        date = getYesterday()
        print(num, code, name, closing_price, up_down, balance, proportion, purchase, sales, chazhi, rq_balance,
              rq_quantity, rq_maichu, rq_changhuan, rq_chazhi, rzrq_balance, rzrq_balance_chazhi)
        num =int(num)
        closing_price=float(closing_price)
        up_down=float(up_down)
        if flag == 'SH':
            code = code + '.SH'
            cur.execute(
                "INSERT INTO rzrq (date,sym,symname,close,yield,rzye,rzyezb,rzmre,rzche,rzjme,rqye,rqyl,rqmcl,rqchl,rqjmc,rzrqye,rzrqce) VALUES (date,code,num,name,closing_price,up_down,balance,proportion,purchase,sales,chazhi,rq_balance,rq_quantity,rq_maichu,rq_changhuan,rq_chazhi,rzrq_balance,rzrq_balance_chazhi)")
        else:
            code = code + '.SZ'
            cur.execute("INSERT INTO rzrq (date,sym,symname,close,yield,rzye,rzyezb,rzmre,rzche,rzjme,rqye,rqyl,rqmcl,rqchl,rqjmc,rzrqye,rzrqce) VALUES (date,code,num,name,closing_price,up_down,balance,proportion,purchase,sales,chazhi,rq_balance,rq_quantity,rq_maichu,rq_changhuan,rq_chazhi,rzrq_balance,rzrq_balance_chazhi)")


def today():
    date =  getYesterday()
    url_sh = 'http://data.eastmoney.com/rzrq/detail/SH.{}.html'.format(date)
    url_sz = 'http://data.eastmoney.com/rzrq/detail/SZ.{}.html'.format(date)
    urllist = [url_sh, url_sz]
    driver = get_driver()
    for url in urllist:
        if 'SH' in url_sh:
            flag = 'SH '
        else:
            flag = 'SZ'
        driver.get(url)
        time.sleep(3)
        html = driver.page_source
        jx = etree.HTML(html)
        ending_num = int(jx.xpath('//*[@id="PageCont"]/a[@title="转到最后一页"]/text()')[0])
        parse(jx, flag,date)
        # print(ending_num)
        for num in range(ending_num - 1):
            num = num + 2
            # time.sleep(5)
            print('----------------------------------------------正在爬取第{}页'.format(num))
            driver.find_element_by_xpath('//*[@id="PageContgopage"]').clear()
            driver.find_element_by_xpath('//*[@id="PageContgopage"]').send_keys(num)
            driver.find_element_by_xpath('//*[@id="PageCont"]/a[9]').click()
            time.sleep(2)
            html = driver.page_source
            jx = etree.HTML(html)
            parse(jx, flag,date)


def history():
    for i in redis_conn.llen('date'):
        date = redis_conn.rpop('date')
        url_sh = 'http://data.eastmoney.com/rzrq/detail/SH.{}.html'.format(date)
        url_sz = 'http://data.eastmoney.com/rzrq/detail/SZ.{}.html'.format(date)
        urllist = [url_sh, url_sz]
        driver = get_driver()
        for url in urllist:
            if 'SH' in url_sh:
                flag = 'SH '
            else:
                flag = 'SZ'
            driver.get(url)
            time.sleep(3)
            html = driver.page_source
            jx = etree.HTML(html)
            ending_num = int(jx.xpath('//*[@id="PageCont"]/a[@title="转到最后一页"]/text()')[0])
            parse(jx, flag,date)
            # print(ending_num)
            for num in range(ending_num - 1):
                num = num + 2
                # time.sleep(5)
                print('----------------------------------------------正在爬取第{}页'.format(num))
                driver.find_element_by_xpath('//*[@id="PageContgopage"]').clear()
                driver.find_element_by_xpath('//*[@id="PageContgopage"]').send_keys(num)
                driver.find_element_by_xpath('//*[@id="PageCont"]/a[9]').click()
                time.sleep(2)
                html = driver.page_source
                jx = etree.HTML(html)
                parse(jx, flag,date)


if __name__ == '__main__':
    """
    today()#爬取今日数据
    create_assist_date(datestart=20110101, dateend=20190716)#创建日期列表存入redis
    history() #爬取历史数据

    """
    # today()
    fire.Fire()
