# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import sqlite3


class DatabasePipeline(object):

    def __init__(self):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.connection = sqlite3.connect(path + '/integrated/database.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute('DROP TABLE IF EXISTS profiles_test')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS profiles_test '
                            '(url TEXT PRIMARY KEY, '
                            'person_name TEXT, '
                            'department TEXT,'
                            'university TEXT,'
                            'tag TEXT,'
                            'position_title TEXT,'
                            'phd_year TEXT,'
                            'phd_school TEXT,'
                            'promotion_year TEXT,'
                            'text_raw TEXT)')

    def process_item(self, item, spider):
        self.cursor.execute("SELECT * FROM profiles_test WHERE url=?", (item['profile_link'], ))
        result = self.cursor.fetchone()
        if not result:
            self.cursor.execute(
                "INSERT INTO profiles_test (url, person_name, department, university, tag, position_title, phd_year, phd_school, "
                "promotion_year, text_raw) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (item['profile_link'], item['name'], item['department'], item['university'], item['tag'],
                 item['position'], item['phd_year'], item['phd_school'], item['promotion_year'], ' '.join(item['text_raw'])))
            self.connection.commit()
        return item
