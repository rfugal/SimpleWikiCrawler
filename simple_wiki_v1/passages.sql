CREATE TABLE passages ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'passage' STRING NOT NULL, 'link' STRING UNIQUE NOT NULL, 'title' STRING NOT NULL);
CREATE TABLE passage_uniq_words ('id' INT NOT NULL, 'word_count' INT NOT NULL, 'uniq_count' INT NOT NULL, 'uniq_q2' INT DEFAULT 0, 'uniq_q3' INT DEFAULT 0, 'uniq_q4' INT DEFAULT 0, FOREIGN KEY(id) REFERENCES passages(id));
CREATE TABLE passage_words ('id' INT NOT NULL, 'words_q2' STRING, 'words_q3' STRING, 'words_q4' STRING, FOREIGN KEY(id) REFERENCES passages(id));
CREATE TABLE passage_word_model ('id' INT NOT NULL, 'word_model' STRING NOT NULL, 'freq_score' INT, FOREIGN KEY(id) REFERENCES passages(id));
CREATE TABLE spider_queue ('url' STRING PRIMARY KEY, 'parsed' INT DEFAULT 0, 'time' TIMESTAMP);
CREATE TABLE word_frequency ('word' STRING NOT NULL PRIMARY KEY, 'frequency' INT, 'quartile' INT NOT NULL DEFAULT 4, 'index' INT);