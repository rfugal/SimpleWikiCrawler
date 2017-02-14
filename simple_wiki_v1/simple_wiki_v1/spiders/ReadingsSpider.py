# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 08:47:38 2017

@author: russ.fugal
"""
from bs4 import BeautifulSoup
import scrapy
import sqlite3
import re
import json

def StartUrl():
	prefix = 'https://simple.wikipedia.org/wiki/'
	try:
		db = sqlite3.connect('passages.db')
		c = db.cursor()
		url_query = 'select url from spider_queue where parsed=0 limit 100'
		c.execute(url_query)
		m = c.fetchall()
		urls = []
		for n in m:
			urls.append(prefix + n[0])
		return urls
	finally:
		c.close()
		db.close()

def WordArray(self, title, passage, link):
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
			word_cursor.execute('select quartile, frequency from word_frequency where word=:word',{'word':word})
			r = word_cursor.fetchone()
			if r is not None:
				uniq_words[word] = r[0]
				word_freq[word] = r[1]
		freq_score += word_freq[word]

	uniq_count = len(uniq_words)
	if word_count == 0:
		freq_score = 0
	else:
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
		ins_passage = "insert into passages ('title', 'passage', 'link') values (?, ?, ?)"
		word_cursor.execute(ins_passage, (title, passage, link))
		rowid = word_cursor.lastrowid
		word_cursor.execute('select id from passages where rowid=:id',{'id': rowid})
		passageid = word_cursor.fetchone()[0]
	
		ins_uniq = "insert into passage_uniq_words values (?,?,?,?,?,?)"
		word_cursor.execute(ins_uniq,(passageid,word_count,uniq_count,len(uniq_q2),len(uniq_q3),len(uniq_q4)))
			
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

def VisitedLink(self, link):
	link = link[34:len(link)]
	db = sqlite3.connect("passages.db")
	c = db.cursor()
	try:
		query = "select url from spider_queue where url=:link"
		c.execute(query, {'link': link})
		result = c.fetchone()
		if (result is None):
			ins = "insert into spider_queue (url, parsed, time) values (?, 1, current_timestamp)"
			c.execute(ins, (link))
			db.commit()
		else:
			upd = "update spider_queue set parsed=1, time=current_timestamp where url=:link"
			c.execute(upd, {'link': link})
			db.commit()
	finally:
		c.close()
		db.close()

def UnvisitedLink(self, link):
	link = link[34:len(link)]
	db = sqlite3.connect("passages.db")
	c = db.cursor()
	response = False
	try:
		query = "select url, parsed from spider_queue where url=:link"
		c.execute(query, {'link': link})
		result = c.fetchone()
		if (result is None):
			ins = "insert into spider_queue ('url', 'time') values (:link, current_timestamp)"
			c.execute(ins, {'link': link})
			db.commit()
			response = True
		elif (result[1] == 0):
			response = True
	finally:
		c.close()
		db.close()
	
	return response

class ReadingsSpider(scrapy.Spider):
	name = "wikipedia_passages"
	start_urls = StartUrl()
		
	def parse(self, response):
		# check if link has been visited previously
		if (UnvisitedLink(self, response.url)):
			try:
				# get first paragraph of Wikipedia article
				markup = response.css('div#mw-content-text p').extract_first()
				# extract text
				soup = BeautifulSoup(markup, 'lxml')
				paragraph = soup.get_text()
				# extract subject title
				title = response.css('div#mw-content-text p b::text').extract_first()
				# write paragraph, title, words, and response.url to database
				WordArray(self, title, paragraph, response.url)
				# record visited link to database
		
				# add links in article body to webcrawler
				for nextLink in response.css('div#mw-content-text p a::attr("href")').extract():
					if (nextLink[0:6] == "/wiki/" and nextLink[6:10] != "File" and nextLink[6:10] != 'Talk' and not None and nextLink[6:15] != "User_talk" and nextLink[6:15] != 'Wikipedia' and not re.match('#', nextLink) and  not re.match('/', nextLink[6:len(nextLink)])):
						# add link to list
						nextLink = response.urljoin(nextLink)
						# check if link has been visited previously
						if (UnvisitedLink(self, nextLink)):
							yield scrapy.Request(nextLink, callback=self.parse)
			finally:
				VisitedLink(self, response.url)
