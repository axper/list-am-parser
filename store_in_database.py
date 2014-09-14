#! /usr/bin/env python2
# -*- coding: utf-8 -*-

''' Parses category at www.list.am and stores into a database
'''

from __future__ import print_function
import bs4
import requests
import re
import psycopg2
import datetime


DOLLAR_TO_DRAM = 410


class Database(object):
    def __init__(self):
        self.conn = psycopg2.connect('dbname=list_am')

    def create_listam_table(self):
        cur = self.conn.cursor()

        cur.execute('create table listam (id serial PRIMARY KEY,'
                    '                     title varchar,'
                    '                     price bigint,'
                    '                     location varchar,'
                    '                     phone varchar,'
                    '                     user_id varchar,'
                    '                     date date,'
                    '                     description varchar'
                    '                    );')
        self.conn.commit()

        cur.close()

    def item_exists(self, item_id):
        cur = self.conn.cursor()

        cur.execute('select 1 from listam where id=(%s) limit 1;', (item_id,))
        self.conn.commit()

        fetchall_result = cur.fetchall()
        self.conn.commit()

        cur.close()

        if fetchall_result:
            return True
        else:
            return False

    def add_line(self, info):
        cur = self.conn.cursor()

        try:
            cur.execute('insert into listam values ('
                        '%(id)s,'
                        '%(title)s,'
                        '%(price)s,'
                        '%(location)s,'
                        '%(phone)s,'
                        '%(user_id)s,'
                        '%(date)s,'
                        '%(description)s);',
                        info)
        except psycopg2.IntegrityError:
            print('Duplicate entry (shouldnt have happened):', info['item_id'])
            self.conn.rollback()
        else:
            print(info)
            self.conn.commit()

        cur.close()


db = Database()
#db.create_listam_table()



class ItemParser(object):
    def get_url(self, item_id):
        return 'http://www.list.am/item/' + item_id

    def get_title(self):
        return self.soup.h1.text

    def get_price(self):
        try:
            price_text = self.soup.find(class_='price').text
        except AttributeError:
            return None

        match_dram = re.search(u'^([,0-9]*) դրամ$', price_text)

        if match_dram:
            dram_string = match_dram.groups()[0]
            dram = int(dram_string.replace(',', ''))

            return dram
        else:
            match_dollar = re.search(u'^\$([,0-9]*)$', price_text)

            if match_dollar:
                dram_string = match_dollar.groups()[0]
                dram = int(dram_string.replace(',', ''))
                dram *= DOLLAR_TO_DRAM

                return dram
            else:
                print('ERROR: UNKNOWN PRICE FORMAT:', price_text)

                return 0

    def get_location(self):
        try:
            location = self.soup.find(class_='loc').text
        except AttributeError:
            location = None

        return location

    def get_phone(self):
        try:
            phone = self.soup.find('div', class_='phone').text
        except AttributeError:
            phone = None

        return phone

    def get_user_id(self):
        from_user_tag = self.soup.find('a', href=re.compile('^/from-user/'))
        user_partial_link = from_user_tag['href']
        match = re.match('^/from-user/([0-9].*)', user_partial_link)
        user_id = match.groups()[-1]

        return user_id

    def get_date(self):
        footer_string = self.soup.find('div', class_='footer').text
        date_string = footer_string.split()[-1]
        day_month_year = date_string.split('.')
        date = datetime.date(int(day_month_year[2]),
                             int(day_month_year[1]),
                             int(day_month_year[0])
                            )

        return date

    def get_description(self):
        description = self.soup.find('div', class_='body').text

        return description

    def get_info(self, item_id):
        ''' Returns a dictionary containing all info about given item '''
        url = self.get_url(item_id)

        self.soup = get_soup(url)

        info = {}
        info['id'] = item_id
        info['title'] = self.get_title()
        info['price'] = self.get_price()
        info['location'] = self.get_location()
        info['phone'] = self.get_phone()
        info['user_id'] = self.get_user_id()
        info['date'] = self.get_date()
        info['description'] = self.get_description()

        return info


def get_item_id_from_href_string(href_string):
    match = re.match('/item/([0-9].*)', href_string)
    item_id = match.groups()[-1]

    return item_id


def get_html_text(url):
    response = requests.get(url)

    return response.text


def get_soup(url):
    html_text = get_html_text(url)
    soup = bs4.BeautifulSoup(html_text)

    return soup


def parse_page(page_number):
    # 98: category
    # {}: page number
    # n=1: yerevan
    # type=0: offer
    print('Parsing page #' + str(page_number))

    soup = get_soup('http://www.list.am/category/98/{}?n=1&type=1'.format(page_number))

    all_links = soup.find_all('a')

    for link in all_links:
        text = link.string
        href = link['href']


        if text is not None and href.startswith('/item/'):
            item_id = get_item_id_from_href_string(href)

            if not db.item_exists(item_id):
                item_parser = ItemParser()

                info = item_parser.get_info(item_id)
                db.add_line(info)


def main():
    for i in range(1, 135):
        parse_page(i)


if __name__ == '__main__':
    main()

