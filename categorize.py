#! /usr/bin/env python2
# -*- coding: utf-8 -*-

''' Parses the database
'''

from __future__ import print_function
import bs4
import requests
import re
import datetime
import psycopg2
import psycopg2.extensions
import known_phrases

# Fuck everything that is not unicode
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class CategoryGuesser(object):
    armenian_alphabet = u'աԱբԲգԳդԴեԵզԶէԷըԸթԹժԺիԻլԼխԽծԾկԿհՀձՁղՂճՃմՄ'\
                        u'յՅնՆշՇոՈչՉպՊջՋռՌսՍվՎտՏրՐցՑոՈւՒփՓքՔևևօՕՖֆ'
    russian_alphabet = u'АаБбВвГгДдЕеЁёЖжЗзИиЙйКкЛлМмНнОо'\
                       u'ПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя'

    @staticmethod
    def contains_both_numbers_and_letters(word):
        regex_contains_letters = u'[a-zA-Z' +\
                                 CategoryGuesser.armenian_alphabet +\
                                 CategoryGuesser.russian_alphabet +\
                                 u']'
        regex_contains_numbers = u'[0-9]'

        contains_numbers = re.search(regex_contains_numbers, word)
        contains_letters = re.search(regex_contains_letters, word)

        if contains_numbers and contains_letters:
            return True
        else:
            return False

    @staticmethod
    def get_phrase_category(phrase):
        if phrase.isdigit():
            return 'number'

        if len(phrase) <= 2:
            return 'toosmall'

        if CategoryGuesser.contains_both_numbers_and_letters(phrase):
            return 'model'

        '''
        for key in known_phrases.known_phrases.iterkeys():
            if phrase in known_phrases.known_phrases[key]:
                return key
        '''

        for category, category_phrases in known_phrases.known_phrases.items():
            if phrase in category_phrases:
                return category

        return 'UNKNOWN_PHRASE'

    @staticmethod
    def get_title_phrases(title):
        ''' Returns a list containing the words found in
            the title in lowercase and stripped non-alnum chars
        '''
        regex_string = u'[^0-9a-zA-Z' +\
                       CategoryGuesser.armenian_alphabet +\
                       CategoryGuesser.russian_alphabet +\
                       u']'
        
        special_chars_removed = re.sub(regex_string, ' ', title)

        words_lowercase = special_chars_removed.lower()

        phrases = words_lowercase.split()

        return phrases

    @staticmethod
    def get_categories(title):
        ''' Returns an array of tuples
            phrase the phrase and the corresponding category
        '''
        phrases = CategoryGuesser.get_title_phrases(title)
        categories = []

        for phrase in phrases:
            category = CategoryGuesser.get_phrase_category(phrase)
            categories.append((phrase, category))

        return categories


class Database1(object):
    def __init__(self):
        self.conn = psycopg2.connect('dbname=list_am')
        self.cur = self.conn.cursor()

    def clean_table_item_categories(self):
        self.cur.execute('delete '
                         'from item_categories;'
                        )
        self.conn.commit()

    def categorize(self):
        self.clean_table_item_categories()

        self.cur.execute('select id, title '
                         'from listam;'
                        )
        fetchall_result = self.cur.fetchall()
        self.conn.commit()

        args_str = ''

        for row in fetchall_result:
            item_id = row[0]
            title = row[1]

            # Sanity check
            if title is None:
                continue

            phrase_and_category_tuples = CategoryGuesser.get_categories(title)

            for phrase_and_category in phrase_and_category_tuples:
                phrase = phrase_and_category[0]
                category = phrase_and_category[1]

                final_row = (item_id, phrase, category)

                args_str += self.cur.mogrify('(DEFAULT,%s,%s,%s)', final_row) + ','

        args_str = args_str[:-1]
        self.cur.execute('insert into item_categories (id, item_id, phrase, category) values '
                         + args_str + ';')

        self.conn.commit()

        self.cur.close()


categories_neutral = [
    u'neutral',
    u'multiple',
    u'brand',
    u'param',
    u'buzzwords',
    u'state',
    u'UNKNOWN_PHRASE',
    u'number',
    u'toosmall',
    u'model',
    u'notebook_part',
    u'part',
]
categories_unwanted = [
    u'buyer',
    u'unwanted',
    u'wrong_category',
]
categories_real = [
    u'case',
    u'motherboard',
    u'cpu',
    u'fan',
    u'ram',
    u'ram_old',
    u'chips',
    u'ssd',
    u'gpu',
    u'hdd',
    u'cooler',
    u'notebook',
    u'monitor',
    u'dvd',
    u'mouse',
    u'sound_system',
    u'headphone',
    u'microphone',
    u'camera',
    u'flash',
    u'net',
    u'modem',
    u'computer',
    u'cartridge',
    u'wireless',
    u'bluetooth',
    u'psu',
    u'adapter',
    u'battery',
    u'ups',
    u'printer',
    u'xerox',
    u'scanner',
    u'tv_tuner',
    u'table',
    u'chair',
    u'cable',
    u'bag',
    u'keyboard',
    u'thermal_paste',
    u'protection',
    u'tablet',
    u'antenna',
    u'game',
    u'detail',
]


class Database2(object):
    def __init__(self):
        self.conn = psycopg2.connect('dbname=list_am')
        self.cur = self.conn.cursor()

    def update_category(self, item_id, category):
        self.cur.execute('update listam '
                         'set category = %s '
                         'where id = %s;',
                         (category, item_id,))

    def get_main_categories(self, list_of_categories):
        useful_count = {}

        for category in list_of_categories:
            if category in categories_unwanted:
                return {category : 1}

            if category in categories_real:
                try:
                    useful_count[category] += 1
                except KeyError:
                    useful_count[category] = 1

        return useful_count

    def get_main_category(self, main_categories):
        if not main_categories:
            return None

        max_key = max(main_categories, key=main_categories.get)

        return max_key

    def categorize(self):
        self.cur.execute('select listam.id, item_categories.category '
                         'from listam join item_categories '
                         'on listam.id = item_categories.item_id;'
                        )
        fetchall_result = self.cur.fetchall()
        self.conn.commit()

        result_dict = {}
        for i in fetchall_result:
            if len(i) < 2:
                continue
            result_dict.setdefault(i[0], []).append(i[1])

        for item_id in result_dict.keys():
            categories = result_dict[item_id]

            main_categories = self.get_main_categories(categories)
            main_category = self.get_main_category(main_categories)
            self.update_category(item_id, main_category)

        self.conn.commit()
        self.cur.close()

def main():
    db1 = Database1()
    db1.categorize()

    db2 = Database2()
    db2.categorize()

if __name__ == '__main__':
    main()

