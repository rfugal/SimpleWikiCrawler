# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 22:10:18 2017

@author: russ.fugal
"""

import sys
import sqlite3
import getopt
import re
import json

TABLE_NAME = 'autocomplete_model'
COLUMN_STRING = 'string'
COLUMN_WORD = 'word_boolean'
COLUMN_PARENT = 'parent_string'
COLUMN_CHILDREN = 'children'
COLUMN_OPTIONS = 'options_count'
COLUMN_FREQ = 'children_count'
COLUMN_LENGTH = 'string_length'
empty = dict()

CREATE_TABLE = " (" + COLUMN_STRING + " STRING PRIMARY KEY, " + COLUMN_WORD + " BOOLEAN NOT NULL, " 
CREATE_TABLE = CREATE_TABLE + COLUMN_PARENT + " STRING, " + COLUMN_CHILDREN + " STRING DEFAULT \"" + json.dumps(empty) + "\", "  
CREATE_TABLE = CREATE_TABLE + COLUMN_OPTIONS + " INT DEFAULT 0, " + COLUMN_FREQ  + " INT DEFAULT 0, " + COLUMN_LENGTH + " INT)"
CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + CREATE_TABLE
    
def processWord(word,count,tier,database_filename):
    children = dict()
    if tier < len(word):
        test = False
        substring = word[:tier]
        children[word[tier]] = count
    elif tier == len(word):
        test = True
        substring = word
    else:
        print('fail')
        return
    parent = word[:tier-1]
    length = len(substring)
    options = 1
    subcount = count
    try:
        db = sqlite3.connect(database_filename)
        c = db.cursor()
        query = "SELECT * FROM " + TABLE_NAME + " WHERE " + COLUMN_STRING + "=:string"
        c.execute(query, {'string':substring})
        m = c.fetchone()
        if m is not None:
            if m[1] is True:
                test = True
            add_children = json.loads(m[3])
            for child in children:
                if child in add_children:
                    add_children[child] += count
                else:
                    add_children[child] = count
            children = json.dumps(add_children)
            options = len(add_children)
            subcount += m[5]
            update = "UPDATE " + TABLE_NAME + " SET " + COLUMN_WORD + "=:word, " + COLUMN_CHILDREN + "=:children, "
            update = update + COLUMN_OPTIONS + "=:options, " + COLUMN_FREQ + "=:count WHERE " + COLUMN_STRING + "=:string"
            c.execute(update, {'word':test, 'children':children, 'options':options, 'count':subcount, 'string':substring})
            db.commit()
        else:
            children = json.dumps(children)
            insert = "INSERT INTO " + TABLE_NAME + " VALUES (?,?,?,?,?,?,?)"
            c.execute(insert, (substring,test,parent,children,options,subcount,length))
            db.commit()
    finally:
        c.close()
        db.close()
    if tier < len(word):
        tier += 1
        processWord(word,count,tier,database_filename)
    else:
        return

def createDatabase(database_filename,source_filename):
    try:
        db = sqlite3.connect(database_filename)
        c = db.cursor()
        c.execute(CREATE_TABLE)
        db.commit()
    finally:
        c.close()
        db.close()
    
    try:
        words = json.load(open(source_filename))
    except ValueError:
        print ('JSON extraction of Source Failed, attempting to extract words. Integer values will be lost.')
        with open(source_filename, 'r') as myfile:
            raw = myfile.read().replace('\n', '')
        words = dict()
        m = re.findall("([A-Za-z']+)", raw)
        for word in m:
            if word in words:
                words[word] += 1
            else:
                words[word] = 1
    for word in words:
        processWord(word.lower(),words[word],1,database_filename)

def usage():
    print('TBD')

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "s:d:", ["source=","database="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-s","--source"):
            source_filename = arg
        if opt in ("-d","--database"):
            database_filename = arg
    createDatabase(database_filename,source_filename)

if __name__ == "__main__":
    main(sys.argv[1:])
