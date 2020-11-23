import requests
import re
import bs4
import datetime as dt
import time
import sqlite3
from multiprocessing.dummy import Pool as TPool


class parser:
    """Загрузчик новостей с investing.com."""

    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux "
                                      "x86_64; rv:82.0) Gecko/20100101 "
                                      "Firefox/82.0"}

    def start(self):
        """Запуск парсера."""
        variant = [('https://ru.investing.com/news/forex-news/',
                    'Новости валютного рынка',
                    'forex'),
                   ('https://ru.investing.com/news/commodities-news/',
                    'Новости фьючерсов и сырьевых рынков',
                    'commodities'),
                   ('https://ru.investing.com/news/stock-market-news/',
                    'Новости фондовых рынков',
                    'market'),
                   ('https://ru.investing.com/news/economic-indicators/',
                    'Экономические показатели',
                    'indicators'),
                   ('https://ru.investing.com/news/economy/',
                    'Новости экономики',
                    'economy'),
                   ('https://ru.investing.com/news/cryptocurrency-news/',
                    'Новости криптовалют',
                    'crypto')
                   ]
        db = sqlite3.connect('investing.sqlite')
        cur = db.cursor()
        sql = """create table if not exists status (
                    address text primary key,
                    last_page integer not null) ;
              """
        cur.execute(sql)
        db.commit()

        message = 'Выберите что загружать:\n'
        message += '0) загрузить все\n'
        for i in range(len(variant)):
            message += '{}) {} - {}\n'.format(i + 1,
                                              variant[i][1],
                                              variant[i][0])
        print(message)
        while True:
            try:
                ans = int(input('номер: ')) - 1
                address, table_name = variant[ans][0], variant[ans][2]
                break
            except Exception:
                print('введите число от 1 до {}'.format(len(variant)))

        if ans != -1:
            self.load(address, table_name)
        else:
            threads = input('сколько тредов использовать (рекомендуется 1): ')
            p = TPool(int(threads))
            p.starmap(self.load, [(x[0], x[2]) for x in variant])
            p.close()
            p.join()

    def load(self, address: str, table: str):
        """Метод загружает новости по выбранному адресу."""

        db = sqlite3.connect('investing.sqlite')
        cur = db.cursor()
        sql = """
              create table if not exists {} (
                  id integer primary key autoincrement,
                  date text not null,
                  author text not null,
                  title text not null,
                  about text not null,
                  full text not null,
                  url text not null UNIQUE);
              """.format(table)
        cur.execute(sql)
        db.commit()

        # определение номера страницы, на которой остановились
        page = cur.execute('''select last_page
                              from status
                              where address=?
                           ''', [address]).fetchall()
        if len(page) == 0:
            cur.execute('''insert into status (address, last_page)
                           values(?,?);
                        ''', [address, 1])
            db.commit()
            page = 1
        else:
            page = int(page[0][0])
        # print(page)

        while True:
            r = None
            for i in range(10):
                try:
                    r = requests.get(address + str(page), headers=self.headers)
                    break
                except Exception:
                    print(i, 'load failed, trying again...')
                    time.sleep(60)
            if r is None:
                print('Загрузка не удалась', address + str(page))
                break
            html = r.text
            soup = bs4.BeautifulSoup(html, 'html.parser')

            # условие завершения загрузки
            if re.findall('Запрошенная вами страница не существует', html):
                # print('выход по "Запрошенная вами страница не существует"')
                break
            elif r.url != address + str(page) and page != 1:
                # print('выход по "редиректу на первую страницу"')
                break

            mydivs = soup.findAll("div", {"class": "largeTitle"})
            if len(mydivs) != 0:
                mydivs = mydivs[0]
            else:
                # print('debug пропуск mydivs')
                page += 1
                continue

            a = mydivs.find_all('article')
            for article in a:
                article = str(article)
                try:
                    title = re.findall(r'title=".*?">(.+?)</a>', article)[0]
                    date = re.findall(r'"date">(.+?)</span>', article)[0][3:]

                    url = re.findall(r'href="(.+?)"', article)[0]
                    if 'https' not in url:
                        url = 'https://ru.investing.com' + url
                    author = re.findall(r'class="articleDetails"><span>(.+?)</span>', article)[0]
                    about = re.findall(r'<p>(.+?)</p>', article.replace('\n', ''))[0]

                    if 'назад' in date:
                        date = dt.datetime.now().strftime('%d.%m.%Y')
                    full = ''

                    sql = """insert or ignore into {}
                             (date, author, title, about, full, url)
                             values(?,?,?,?,?,?);""".format(table)
                    cur.execute(sql, [date, author, title, about, full, url])

                except Exception:
                    pass
            print(address + str(page), 'loaded')
            cur.execute('''update status
                           set last_page=?
                           where address=?
                        ''', [page, address])
            db.commit()
            page += 1
        print('>>> Загрузка {} завершена'.format(address))


if __name__ == '__main__':
    p = parser()
    p.start()
