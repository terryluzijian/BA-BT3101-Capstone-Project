# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import sqlite3
import datetime


class DatabasePipeline(object):

    crawler_name = 'core'

    def __init__(self):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.connection = sqlite3.connect(path + '/integrated/database.db')
        self.cursor = self.connection.cursor()

        self.cursor.execute('CREATE TABLE IF NOT EXISTS process'
                            '(crawler_name TEXT PRIMARY KEY, '
                            'processing INT NOT NULL )')
        self.cursor.execute('SELECT * FROM process WHERE crawler_name=?', (DatabasePipeline.crawler_name, ))
        crawler_exist = self.cursor.fetchone()
        if not crawler_exist:
            self.cursor.execute('INSERT INTO process (crawler_name, processing) values (?, ?)',
                                (DatabasePipeline.crawler_name, 1))
        else:
            self.cursor.execute('UPDATE process SET processing = ? WHERE crawler_name = ?', (1, DatabasePipeline.crawler_name))

        self.cursor.execute('CREATE TABLE IF NOT EXISTS profiles '
                            '(profile_link TEXT PRIMARY KEY, '
                            'name TEXT, '
                            'department TEXT,'
                            'university TEXT,'
                            'tag TEXT,'
                            'position TEXT,'
                            'phd_year TEXT,'
                            'phd_school TEXT,'
                            'promotion_year TEXT,'
                            'text_raw TEXT)')

    def process_item(self, item, spider):
        self.cursor.execute("SELECT * FROM profiles WHERE profile_link=?", (item['profile_link'], ))
        result = self.cursor.fetchone()
        if not result:
            self.cursor.execute(
                "INSERT INTO profiles (profile_link, name, department, university, tag, position, phd_year, phd_school, "
                "promotion_year, text_raw) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (item['profile_link'], item['name'], item['department'], item['university'], item['tag'],
                 item['position'], item['phd_year'], item['phd_school'], item['promotion_year'], ' '.join(item['text_raw'])))
            self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cursor.execute('UPDATE process SET processing = ? WHERE crawler_name = ?',
                            (0, DatabasePipeline.crawler_name))
        self.connection.commit()
        self.connection.close()
