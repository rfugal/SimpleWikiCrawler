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
CREATE_TABLE = CREATE_TABLE + COLUMN_PARENT + " STRING, " + COLUMN_CHILDREN + " STRING DEFAULT " + json.dumps(empty) + ", "  
CREATE_TABLE = CREATE_TABLE + COLUMN_OPTIONS + " INT DEFALUT 0, " + COLUMN_FREQ  + " INT DEFALUT 0, " + COLUMN_LENGTH + " INT)"
CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + CREATE_TABLE

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
    
def createDatabase(database_filename,source_filename):
    try:
        db = sqlite3.connect(database_filename)
        c = db.cursor()
        url_query = "SELECT name FROM sqlite_master WHERE type='table'"
        c.execute(url_query)
        m = c.fetchall()
        if TABLE_NAME not in m:
            c.execute(CREATE_TABLE)
            c.commit()
    finally:
        c.close()
        db.close()
    
    try:
        words = json.loads(open(source_filename))
    except ValueError:
        print ('Source is not JSON, attempting to extract words. Integer values will be lost.')
        raw = open(source_filename)
        parse = re.compile("[A-Za-z']+")
        m = parse.matcher(raw)
        while m.movetonext():
            
    
    
def parseSource(source):
    

if __name__ == "__main__":
    main(sys.argv[1:])

def addBuds(freq,t):
    branch = dict()
    for f in freq:
        if len(t) + 1 == len(f) and f[:len(t)] == t:
            branch[f] = dict()
    return (branch)

def buildBranch(freq,tree,stats,depth):
    if depth not in stats['uniq_tiers']:
        stats['uniq_tiers'][depth-1] = 0
        stats['max_options'][depth-1] = 0
    for t in tree:
        tree[t] = addBuds(freq,t)
        stats['uniq_tiers'][depth-1] += 1
        (tree[t],stats) = buildBranch(freq,tree[t],stats,depth + 1)
        if len(tree) > stats['max_options'][depth-1]:
            stats['max_options'][depth-1] = len(tree)
    return (tree,stats)
            
def tallyLetters(words,depth,freq):
    for w in words:
        if len(w) >= depth:
            s = w[:depth]
            if s not in freq:
                freq[s] = 1
            else:
                freq[s] += 1
    return freq

def addNextLetters(tree,freq):
    letters = dict()
    for t in tree:
        letter = t[len(t)-1]
        letters[letter] = freq[t]
    return letters
    
def buildModel(tree,model,freq):
    for t in tree:
        if t not in model:
            model[t] = addNextLetters(tree[t],freq)
            model = buildModel(tree[t],model,freq)
    return model                

def createModel(words):
    s = 1
    stats = dict()
    stats['uniq_tiers'] = dict()
    stats['max_options'] = dict()
    stats['uniq_tiers'][0] = 0
    tree = dict()
    for w in words:
        if w[0] not in tree:
            tree[w[0]] = dict()
        if len(w) > s:
            s = len(w)
    depth = 1
    freq = dict()
    while depth <= s:
        freq = tallyLetters(words,depth,freq)
        depth += 1
    (tree,stats) = buildBranch(freq,tree,stats,1)
    model = dict()
    model = buildModel(tree,model,freq)
    return (freq,tree,model,stats)