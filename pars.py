import requests
import re
import bs4
import datetime as dt
import time
import sqlite3
from multiprocessing.dummy import Pool as TPool
from newspaper import Article


class Db:
    """Прослойка для работы с базой."""

    def __init__(self):
        self.db_path = 'investing.sqlite'

    def init_db(self, tables):
        """Создает в базе таблицы по переданному списку имен таблиц."""
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        sql = """create table if not exists status (
                   table_name text primary key,
                   last_page integer not null);
              """
        cur.execute(sql)
        for table in tables:
            sql = """create table if not exists {} (
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
        db.close()

    def get_last_page(self, table):
        """Возвращает номер последней загруженной страницы."""
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        page = cur.execute('''select last_page
                              from status
                              where table_name=?
                           ''', [table]).fetchall()
        if len(page) == 0:
            cur.execute('''insert into status (table_name, last_page)
                             values(?,?);
                        ''', [table, 1])
            db.commit()
            page = 1
        else:
            page = int(page[0][0])
        db.close()
        return page

    def add_news(self, table, date, author, title, about, full, url):
        """Добавляет в базу одну новость."""
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        sql = """insert or ignore into {}
                 (date, author, title, about, full, url)
                 values(?,?,?,?,?,?);""".format(table)
        cur.execute(sql, [date, author, title, about, full, url])
        db.commit()
        db.close()

    def update_last_page(self, table, page):
        """Обновляет номер последней загруженной страницы."""
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        cur.execute('''update status
                       set last_page=?
                       where table_name=?
                    ''', [page, table])
        db.commit()
        db.close()

    def get_news_without_full(self, table):
        """Возвращает статьи без полного текста.

        Возвращает список кортежей [(id, url), ...]
        """
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        need_load = cur.execute("""select id, url
                                     from {}
                                     where full='';
                                """.format(table)).fetchall()
        db.close()
        return need_load

    def update_full_text(self, table, id_, full):
        """Обновляет полный текст статьи по id."""
        db = sqlite3.connect(self.db_path)
        cur = db.cursor()
        cur.execute('''update {}
                       set full=?
                       where id=?
                    '''.format(table), [full, id_])
        db.commit()
        db.close()


class Parser:
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

        msg = "Загружать полный текст статей? (y/n)\n"
        msg += "это может занять много времени: "
        load_full = input(msg)
        load_full = True if load_full == 'y' else False

        if ans != -1:
            Db().init_db([table_name])
            self.load(address, table_name, load_full)
        else:
            threads = input('сколько тредов использовать (рекомендуется 4): ')
            Db().init_db([x[2] for x in variant])
            p = TPool(int(threads))
            p.starmap(self.load, [(x[0], x[2], load_full) for x in variant])
            p.close()
            p.join()

    def load(self, address: str, table: str, load_full: bool):
        """Метод загружает новости по выбранному адресу."""

        db = Db()

        # определение номера страницы, на которой остановились
        page = db.get_last_page(table)

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

                    # загрузка полного текста статей
                    if load_full:
                        full = self.load_full(url)
                        if len(full) == 0:
                            full = 'bad parse'
                    else:
                        full = ''

                    db.add_news(table, date, author, title, about, full, url)
                    print(table, page, date, author, title)

                except Exception:
                    pass
            db.update_last_page(table, page)
            page += 1

        print('>>> Загрузка {} завершена'.format(address))

    @staticmethod
    def load_full(url):
        """Загружает полный текст статьи по url."""
        for i in range(10):
            full = ''
            try:
                a = Article(url)
                a.download()
                a.parse()
                full = a.text
            except Exception:
                time.sleep(10)
            finally:
                return full


if __name__ == '__main__':
    parser = Parser()
    parser.start()
