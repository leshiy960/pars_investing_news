import requests
import re
import bs4
import datetime as dt
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
            # threads = int(input('сколько тредов использовать?: '))
            threads = 4
            p = TPool(threads)
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
                  page integer not null,
                  date text not null,
                  author text not null,
                  title text not null,
                  about text not null,
                  full text not null,
                  url text not null);
              """.format(table)
        cur.execute(sql)
        db.commit()

        # определение номера страницы, на которой остановились
        page = cur.execute('select max(page) from ' + table).fetchall()[0][0]
        page = int(page) + 1 if page is not None else 1
        # print(page)

        while True:
            r = requests.get(address + str(page), headers=self.headers)
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

                    sql = """insert into {}
                             (page, date, author, title, about, full, url)
                             values(?,?,?,?,?,?,?);""".format(table)
                    cur.execute(sql,
                                [page, date, author, title, about, full, url])

                    # print(page, date, title)
                except Exception:
                    pass
            db.commit()
            print(address + str(page), 'loaded')
            page += 1
        print('>>> Загрузка {} завершена'.format(address))


if __name__ == '__main__':
    p = parser()
    p.start()
