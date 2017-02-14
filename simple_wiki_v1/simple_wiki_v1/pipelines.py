# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
import re
import json

def WordArray(passage, passageid):
    db = sqlite3.connect('passages.db')
    word_cursor = db.cursor()
    words = re.findall("[A-z']+", passage)
    word_count = len(words)
    word_model = dict()
    uniq_words = dict()
    word_freq = dict()
    freq_score = 0
    for word in words:
        word = word.lower()
        if word in word_model:
            word_model[word] += 1
        else:
            word_model[word] = 1
            uniq_words[word] = 4
            word_freq[word] = 1
            try:
                word_cursor.execute('select quartile, frequency from word_frequency where word=:word',{'word':word})
                r = word_cursor.fetchone()
                uniq_words[word] = r[0]
                word_freq[word] = r[1]
            except ValueError:
                print (word)
        freq_score += word_freq[word]
    uniq_count = len(uniq_words)
    freq_score = freq_score / word_count
    uniq_q1 = []
    uniq_q2 = []
    uniq_q3 = []
    uniq_q4 = []
    for word, quartile in uniq_words.items():
        if quartile == 1:
            uniq_q1.append(word)
        elif quartile == 2:
            uniq_q2.append(word)
        elif quartile == 3:
            uniq_q3.append(word)
        else:
            uniq_q4.append(word)
    
    try:
        ins_uniq = "insert into passage_uniq_words values (?,?,?,?,?,?,?)"
        word_cursor.execute(ins_uniq,(passageid,word_count,uniq_count,len(uniq_q1),len(uniq_q2),len(uniq_q3),len(uniq_q4)))
        
        ins_words = "insert into passage_words values (?,?,?,?)"
        uniq_q2 = json.dumps(uniq_q2)
        uniq_q3 = json.dumps(uniq_q3)
        uniq_q4 = json.dumps(uniq_q4)
        word_cursor.execute(ins_words,(passageid,uniq_q2,uniq_q3,uniq_q4))
        
        ins_model = "insert into passage_word_model values (?,?,?)"
        word_model = json.dumps(word_model,separators=(',',':'))
        word_cursor.execute(ins_model,(passageid,word_model,freq_score))
        
        db.commit()
    finally:
        word_cursor.close()
        db.close()

class SimpleWikiV1Pipeline(object):
    
    def open_spider(self, spider):
        self.db = sqlite3.connect('passages.db')

    def close_spider(self, spider):
        self.db.close()        
        
    def process_item(self, item, spider):
        c = self.db.cursor()
        self.logger.info('before')
        ins = "insert into passages ('title', 'passage', 'link') values (?, ?, ?)"
        c.execute(ins, (item['title'], item['passage'], item['link']))
        rowid = c.lastrowid
        c.execute('select id from passages where rowid=:id',{'id': rowid})
        passageid = c.fetchone()[0]
        self.db.commit()
        c.close()
        WordArray(self, item['passage'], passageid)
        self.logger.info('after')
        return item
