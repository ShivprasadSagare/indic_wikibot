from datetime import datetime
import logging
import os
from collections import defaultdict, Counter
import json
import numpy as np


#created custom logger to avoid logging latency in default logger
class ManualLogger():
    def __init__(self, name, log_file_path, use_stdout=False):
        self.name = name
        self.log_file_path = log_file_path
        self.stdout = use_stdout
    
    def info(self, msg):
        present_time = datetime.now()
        msg_str = '%s [INFO] %s' % (present_time.strftime('%m/%d/%Y %I:%M:%S %p'), msg)
        if self.stdout:
            print(msg_str)
        with open(os.path.abspath(self.log_file_path), 'a+') as log_file:
            log_file.write(msg_str+"\n")

    def critical(self, msg):
        present_time = datetime.now()
        msg_str = '%s [CRITICAL] %s' % (present_time.strftime('%m/%d/%Y %I:%M:%S %p'), msg)
        if self.stdout:
            print(msg_str)
        with open(os.path.abspath(self.log_file_path), 'a+') as log_file:
            log_file.write(msg_str+"\n")
    
    def debug(self, msg):
        present_time = datetime.now()
        msg_str = '%s [DEBUG] %s' % (present_time.strftime('%m/%d/%Y %I:%M:%S %p'), msg)
        if self.stdout:
            print(msg_str)
        with open(os.path.abspath(self.log_file_path), 'a+') as log_file:
            log_file.write(msg_str+'\n')
    
    def warn(self, msg):
        present_time = datetime.now()    
        msg_str = '%s [WARN] %s' % (present_time.strftime('%m/%d/%Y %I:%M:%S %p'), msg)
        if self.stdout:
            print(msg_str)    
        with open(os.path.abspath(self.log_file_path), 'a+') as log_file:
            log_file.write(msg_str+'\n')

    def error(self, msg):
        present_time = datetime.now()    
        msg_str = '%s [ERROR] %s' % (present_time.strftime('%m/%d/%Y %I:%M:%S %p'), msg)
        if self.stdout:
            print(msg_str)    
        with open(os.path.abspath(self.log_file_path), 'a+') as log_file:
            log_file.write(msg_str+'\n')        

def create_logger(name, log_file, level=logging.DEBUG):
    """setup logger for each worker"""
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%m/%d/%Y %I:%M:%S %p'))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def clean_str(string):
    return str(string).strip()

def is_key_exists(target, data):
    #check multiple keys from the list
    if isinstance(target, list):
        status = {}
        for item in target:
            status[item] = False
        for item in data:
            if item in target:
                status[item] = True
        res = True
        for _, value in status.items():
            res = res and value
        return res 
    else:
    #checks single key
        for item in data:
            if target == item:
                return True
    return False

#directly copied needs some changes
def load_data(logger, file_path):
    data = set()
    with open(os.path.abspath(file_path), 'r') as d_file:
        for item in d_file.readlines():
            item = clean_str(item)
            if len(item)==0 or item=='':
                continue
            data.add(item)
    logger.debug('loaded data [%d its] from %s' % (len(data), file_path))
    # delete_file(logger, file_path)
    return data

def create_mapping(freq, min_freq=0, max_vocab=50000):
    freq = freq.most_common(max_vocab)
    item2id = {
        '<pad>': 0,
        '<unk>': 1
    }
    offset = len(item2id)
    for i, v in enumerate(freq):
        if v[1] > min_freq:
            item2id[v[0]] = i + offset
    id2item = {i: v for v, i in item2id.items()}
    return item2id, id2item


def create_dict(item_list):
    assert type(item_list) is list
    freq = Counter(item_list)
    return freq

def prepare_mapping(words, min_freq):
    words = [w.lower() for w in words]
    words_freq = create_dict(words)
    word2id, id2word = create_mapping(words_freq, min_freq)
    print("Found %i unique words (%i in total)" % (
        len(word2id), sum(len(x) for x in words)
    ))

    mappings = {
        'word2idx': word2id,
        'idx2word': id2word
    }

    return mappings

def load_text(f, min_freq, max_len):
    with open(f) as jsf:
        txt = json.load(jsf)
    words = []
    new_txt = {}
    for key in txt:
        tmp = []
        for sent in txt[key]:
            tmp.extend(sent)
            tmp.append('<eos>')
        tmp = tmp[:max_len]
        new_txt[key] = tmp
        words.extend(tmp)
    mappings = prepare_mapping(words, min_freq)
    word2idx = mappings["word2idx"]

    vectorize_txt = defaultdict(list)
    for key in new_txt:
        for w in new_txt[key]:
            try:
                vectorize_txt[key].append(word2idx[w])
            except:
                vectorize_txt[key].append(word2idx['<unk>'])
    return mappings, vectorize_txt

def load_dict(f):
    fo = open(f)
    d = {}
    num = int(fo.readline().strip())
    for line in fo:
        line = line.strip()
        name, idd = line.split('\t')
        d[int(idd)] = name
    return d, num

def load_triples(kg_f):
    fo = open(kg_f)
    triples = []
    for line in fo:
        line = line.strip()
        ele = line.split('\t')
        if len(ele)==3:
            ele = list(map(int, ele))
            triples.append(ele)

def load_triple_dict(f):
    fo = open(f)
    triples = []
    triple_dict = defaultdict(set)
    triple_dict_rev = defaultdict(set)
    for line in fo:
        line = line.strip()
        ele = line.split('\t')
        if len(ele) == 3:
            ele = list(map(int, ele))
            triples.append(ele)
            triple_dict[ele[0]].add((ele[1], ele[2]))
            triple_dict_rev[ele[1]].add((ele[0], ele[2]))
    return triples, triple_dict, triple_dict_rev

def bern(triple_dict, triple_dict_rev, tri):
    h = tri[0]
    t = tri[1]
    tph = len(triple_dict[h])
    hpt = len(triple_dict_rev[t])
    deno = tph+hpt
    return tph/float(deno), hpt/float(deno)

def generate_corrupt_triples(pos, num_ent, triple_dict, triple_dict_rev):
    neg = []
    for p in pos:
        sub = np.random.randint(num_ent)
        tph, hpt = bern(triple_dict, triple_dict_rev, p)
        n = [sub, p[1], p[2]]
        chose = np.random.choice(2,1,p=[tph, hpt])
        if chose[0] == 1:
            n = [p[0], sub, p[2]]
        neg.append(n)
    return neg

