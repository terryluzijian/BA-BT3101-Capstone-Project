# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import sqlite3


class DatabaseIOPipeline(object):

    crawler_name = 'core'

    def __init__(self):
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        database_path = os.path.join(parent_path, 'database.db')
        self.connection = sqlite3.connect(database_path)
        self.cursor = self.connection.cursor()

        self.cursor.execute('CREATE TABLE IF NOT EXISTS process'
                            '(crawler_name TEXT PRIMARY KEY, '
                            'processing INT NOT NULL )')
        self.cursor.execute('SELECT * FROM process WHERE crawler_name=?', (DatabaseIOPipeline.crawler_name, ))
        crawler_exist = self.cursor.fetchone()
        if not crawler_exist:
            self.cursor.execute('INSERT INTO process (crawler_name, processing) values (?, ?)',
                                (DatabaseIOPipeline.crawler_name, 1))
        else:
            self.cursor.execute('UPDATE process SET processing = ? WHERE crawler_name = ?', (1, DatabaseIOPipeline.crawler_name))

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
                            'text_raw TEXT,'
                            'user_updated INTEGER)')

    def process_item(self, item, spider):
        self.cursor.execute("SELECT * FROM profiles WHERE profile_link=?", (item['profile_link'], ))
        result = self.cursor.fetchone()
        if not result:
            self.cursor.execute(
                "INSERT INTO profiles (profile_link, name, department, university, tag, position, phd_year, phd_school, "
                "promotion_year, text_raw, user_updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (item['profile_link'], item['name'], item['department'], item['university'], item['tag'],
                 item['position'], item['phd_year'], item['phd_school'], item['promotion_year'],
                 ' '.join(item['text_raw']), 0))
            self.connection.commit()
        else:
            self.cursor.execute(
                "UPDATE profiles SET name = ?, department = ?, university = ?, tag = ?, position = ?, phd_year = ?, phd_school = ?, "
                "promotion_year = ?, text_raw = ? WHERE profile_link = ? and user_updated = 0",
                (item['name'], item['department'], item['university'], item['tag'],
                 item['position'], item['phd_year'], item['phd_school'], item['promotion_year'],
                 ' '.join(item['text_raw']), item['profile_link']))
            self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cursor.execute('UPDATE process SET processing = ? WHERE crawler_name = ?',
                            (0, DatabaseIOPipeline.crawler_name))
        self.connection.commit()
        self.connection.close()
