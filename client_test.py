import httplib
import urllib
import json

# use this server for dev
# SERVER = 'localhost:5000'

# use this server for prod, once it's on ec2
SERVER = '52.10.103.151:5000'

# count module
# count of items in meta table
def get_counts_items(table_name):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/item/counts' % table_name)
    resp = h.getresponse()
    out = resp.read()
    return out

# count of reviews in reviews table
def get_count_reviews(table_name):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/review/counts' % table_name)
    resp = h.getresponse()
    out = resp.read()
    return out

# general statistics module
def get_pricestat(table_name):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/item/pricestat' % table_name)
    resp = h.getresponse()
    out = resp.read()
    return out

def get_ratingstat(table_name):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/review/ratingstat' % table_name)
    resp = h.getresponse()
    out = resp.read()
    return out

def get_helpfulstat(table_name):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/review/helpfulstat' % table_name)
    resp = h.getresponse()
    out = resp.read()
    return out



# rank module
def get_top_price(table_name, num):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/rank/price/%d' % (table_name, num))
    resp = h.getresponse()
    out = resp.read()
    return out

def get_top_rating(table_name, num):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/rank/ratings/%d' % (table_name, num))
    resp = h.getresponse()
    out = resp.read()
    return out

def get_top_weighted_rating(table_name, num):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/rank/weighted_ratings/%d' % (table_name, num))
    resp = h.getresponse()
    out = resp.read()
    return out

def get_top_helpful(table_name, num):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/rank/helpful/%d' % (table_name, num))
    resp = h.getresponse()
    out = resp.read()
    return out

# search module
# search by one asin id
def search_meta_asin_one(table_name, asin):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/item/id/%s' % (table_name, asin))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_meta_asin_one_col(table_name, asin, column):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/item/id/%s/%s' % (table_name, asin, column))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_reviews_asin_one(table_name, asin):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/reviews/id/%s' % (table_name, asin))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_reviews_asin_one_col(table_name, asin, column):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/reviews/id/%s/%s' % (table_name, asin, column))
    resp = h.getresponse()
    out = resp.read()
    return out

# search by one title keyword
def search_meta_title_one(table_name, keyword):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/meta/title/%s' % (table_name, keyword))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_meta_title_one_col(table_name, keyword, column):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/meta/title/%s/%s' % (table_name, keyword, column))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_reviews_title_one(table_name, keyword):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/review/title/%s' % (table_name, keyword))
    resp = h.getresponse()
    out = resp.read()
    return out

def search_reviews_title_one_col(table_name, keyword, column):
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/review/title/%s/%s' % (table_name, keyword, column))
    resp = h.getresponse()
    out = resp.read()
    return out

# search multiple ids with multiple columns, input is a dictionary
def search_meta_id_multi(table_name, params):
    params = json.dumps(params)
    f = urllib.urlopen('http://'+SERVER+'/%s/search/meta/id/' % table_name, params)
    return f.read()

def search_reviews_id_multi(table_name, params):
    params = json.dumps(params)
    f = urllib.urlopen('http://'+SERVER+'/%s/search/review/id/' % table_name, params)
    return f.read()

def search_meta_title_multi(table_name, params):
    try:
        column = "&column="+"&column=".join(params["column"])
        keyword = "&keyword="+"&keyword=".join(params["keyword"])
        query_string = "?"+keyword+column
    except:
        keyword = "&keyword="+"&keyword=".join(params["keyword"])
        query_string = "?"+keyword

    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/meta/title/%s' % (table_name, query_string))
    resp = h.getresponse()
    out = resp.read()
    return out


def search_reviews_title_multi(table_name, params):
    try:
        column = "&column="+"&column=".join(params["column"])
        keyword = "&keyword="+"&keyword=".join(params["keyword"])
        query_string = "?"+keyword+column
    except:
        keyword = "&keyword="+"&keyword=".join(params["keyword"])
        query_string = "?"+keyword

    query_string = "?"+keyword+column
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/%s/search/review/title/%s' % (table_name, query_string))
    resp = h.getresponse()
    out = resp.read()
    return out

if __name__ == '__main__':
    # The books table may be too big to test
    # table_list = ["beauty", "digital_music", "books"]
    table_list = ["beauty", "digital_music"]
    print "************************************************"
    print "Test of MSAN 697 project server running at ", SERVER
    print "created by Yikai Wang"
    print "************************************************"
    print " "
    print "******** counts of items **********"
    for table_name in table_list:
        cache = json.loads(get_counts_items(table_name))
        print table_name+":", cache[table_name]['item_count'], "items"
    print " "
    print "******** counts of reviews **********"
    for table_name in table_list:
        cache = json.loads(get_count_reviews(table_name))
        print table_name+":", cache[table_name]['review_count'], "reviews"
    print " "
    print "******** general statistics **********"
    table_name = 'beauty'
    cache = json.loads(get_pricestat(table_name))
    print "beauty"+" price:", "max =", cache[table_name]['price_stat']['max'], ", min =", cache[table_name]['price_stat']['min'], ", avg =", cache[table_name]['price_stat']['average']
    cache = json.loads(get_ratingstat(table_name))
    print "beauty"+" rating:", "max =", cache[table_name]['rating_stat']['max'], ", min =", cache[table_name]['rating_stat']['min'], ", avg =", cache[table_name]['rating_stat']['average']
    print " "
    print "******** ranked outputs **********"
    table_name = 'beauty'
    rank = 3
    print "beauty: Top %d ranked by price" % rank
    cache = json.loads(get_top_price(table_name, rank))
    print cache[table_name]['price_rank']
    print "beauty: Top %d ranked by average ratings" % rank
    cache = json.loads(get_top_rating(table_name, rank))
    print cache[table_name]['rating_rank']
    print " "
    print "******** single search **********"
    table_name = 'beauty'
    id = '0205616461'
    keyword = 'body'
    column = 'helpful'
    print "beauty meta table:", "search id = %s" % id
    cache = json.loads(search_meta_asin_one(table_name, id))
    print cache[table_name]["search_result"]
    print "beauty reviews table:", "search title keyword = '%s'" % keyword+" with column '%s'" % column
    cache = json.loads(search_reviews_title_one_col(table_name, keyword, column))
    print cache[table_name]["search_result"][0:3]
    print " "
    print "******** multiple search **********"
    table_name = 'beauty'
    id = ['0205616461', '0733001998']
    keyword = ['body', 'lotion']
    column = ['helpful','overall']

    input = {'id':id}
    print "beauty meta table:", "input = ", str(input)
    cache = json.loads(search_meta_id_multi(table_name, input))
    for i in range(len(input['id'])):
        print "search_id", cache[table_name][i]["search_id"], ":", cache[table_name][i]["search_result"]

    input = {'keyword':keyword, "column":column}
    print "beauty reviews table:", "input = ", str(input)
    cache = json.loads(search_reviews_title_multi(table_name, input))
    for i in range(len(input['keyword'])):
        print "search_keyword", cache[table_name][i]["search_keyword"], ":", cache[table_name][i]["search_result"][0:3]
    print " "
