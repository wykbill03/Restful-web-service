# __author__ = Yikai Wang
from flask import Flask, request, jsonify
import psycopg2, psycopg2.extras

# DSN location of the AWS - RDS instance
DB_DSN = "host= dbname= user= password= "

app = Flask(__name__)

# welcome page
@app.route('/')
def default():
    output = dict()
    output['message'] = 'Welcome to the MSAN 697 Project server!'
    output['author'] = 'Yikai Wang'
    output['module'] = [{}]*4
    output['module'][0]={"count":["items", "reviews", "items in reviews table"]}
    output['module'][1]={"general statistics":["price", "rating", "helpful"]}
    output['module'][2]={"ranked outputs":["price", "rating", "weighted rating", "helpful"]}
    output['module'][3]={"search":["by single id", "by single keyword", "by multiple id", "by multiple keyword"],
                         "note":["output column can be specified"]}
    return jsonify(output)


# count module
# count of items
@app.route('/<table_name>/item/counts')
def get_meta_counts(table_name):

    out = dict()
    out[table_name] = dict()
    sql = "SELECT COUNT(*) counts FROM %s" % ("meta_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]['item_count'] = rs['counts']

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# count of reviews
@app.route('/<table_name>/review/counts')
def get_review_counts(table_name):

    out = dict()
    out[table_name] = dict()
    sql = "SELECT COUNT(*) counts FROM %s" % ("reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]['review_count'] = rs['counts']

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# count of items in review table
@app.route('/<table_name>/review/itemcounts')
def get_review_itemcounts(table_name):

    out = dict()
    out[table_name] = dict()
    sql = "SELECT COUNT(DISTINCT asin) counts FROM %s" % ("reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]['review_itemcount'] = rs['counts']

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# general statistics module
# price statistics
@app.route('/<table_name>/item/pricestat')
def get_price_stat(table_name):

    out = dict()
    out[table_name] = dict()
    out[table_name]['price_stat'] = dict()
    sql = \
        """
        SELECT COUNT(price) AS counts, max(price) AS max, min(price) AS min,
        avg(price) AS average, stddev(price) AS std FROM %s
        """ % ("meta_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        # keys = [desc[0] for desc in cur.description]
        # for i in range(len(keys)):
        #     # out[table_name]['price'][keys[i]] = rs[i]
        out[table_name]['price_stat'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# rating statistics
@app.route('/<table_name>/review/ratingstat')
def get_rating_stat(table_name):

    out = dict()
    out[table_name] = dict()
    out[table_name]['rating_stat'] = dict()
    sql = \
        """
        SELECT COUNT(overall) AS counts, max(overall) AS max, min(overall) AS min,
        avg(overall) AS average, stddev(overall) AS std FROM %s
        """ % ("reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]['rating_stat'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# helpful statistics
@app.route('/<table_name>/review/helpfulstat')
def get_helpful_stat(table_name):

    out = dict()
    out[table_name] = dict()
    out[table_name]['helpful_stat'] = dict()
    sql = \
        """
        SELECT COUNT(helpful_rate) AS counts, max(helpful_rate) AS max, min(helpful_rate) AS min,
        avg(helpful_rate) AS average, stddev(helpful_rate) AS std FROM
        (SELECT helpful[1]/helpful[2]::float helpful_rate FROM %s
        WHERE helpful[2] <> 0 AND helpful[1]/helpful[2]::float <= 1) AS inner_q
        """ % ("reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]['helpful_stat'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# rank module
# top prices
@app.route('/<table_name>/rank/price/<num>')
def get_top_price(table_name, num):

    out = dict()
    out[table_name] = dict()
    out[table_name]['top'] = num
    sql = \
        """
        SELECT asin, title, price FROM %s
        WHERE price IS NOT NULL
        ORDER BY price DESC
        LIMIT %s
        """ % ("meta_"+table_name, num)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        out[table_name]['price_rank'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# top avg ratings
@app.route('/<table_name>/rank/ratings/<num>')
def get_top_rating(table_name, num):

    out = dict()
    out[table_name] = dict()
    out[table_name]['top'] = num
    sql = \
        """
        SELECT * FROM
        (SELECT asin, max(overall) max_rating, min(overall) min_rating, avg(overall) avg_rating
        FROM %s
        WHERE overall IS NOT NULL
        GROUP BY asin) LHS
        INNER JOIN
        (SELECT asin, title FROM %s) RHS
        USING(asin)
        ORDER BY avg_rating DESC
        LIMIT %s
        """ % ("reviews_"+table_name, "meta_"+table_name, num)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        out[table_name]['rating_rank'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# top avg weighted ratings, weighted by number of helpful count
@app.route('/<table_name>/rank/weighted_ratings/<num>')
def get_top_weighted_rating(table_name, num):

    out = dict()
    out[table_name] = dict()
    out[table_name]['top'] = num
    sql = \
        """
        SELECT * FROM
        (SELECT asin, sum_rating/sum_helpful::float wt_avg_rating
        FROM
        (SELECT asin, sum(overall*(helpful[1]+1)) sum_rating, sum(helpful[1]+1) sum_helpful FROM %s
        WHERE overall IS NOT NULL AND helpful IS NOT NULL AND helpful[1] <= helpful[2]
        GROUP BY asin) AS inner_q) LHS
        INNER JOIN
        (SELECT asin, title FROM %s) RHS
        USING(asin)
        ORDER BY wt_avg_rating DESC
        LIMIT %s
        """ % ("reviews_"+table_name, "meta_"+table_name, num)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        out[table_name]['wt_rating_rank'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# top avg helpful rate
@app.route('/<table_name>/rank/helpful/<num>')
def get_top_helpful(table_name, num):

    out = dict()
    out[table_name] = dict()
    out[table_name]['top'] = num
    sql = \
        """
        SELECT * FROM
        (SELECT asin, max(helpful_rate) max_helpful_rate, min(helpful_rate) min_helpful_rate,
        avg(helpful_rate) avg_helpful_rate
        FROM
        (SELECT asin, helpful[1]/helpful[2]::float helpful_rate FROM %s
        WHERE helpful[2] <> 0 AND helpful[1]/helpful[2]::float <= 1) AS inner_q
        GROUP BY asin) LHS
        INNER JOIN
        (SELECT asin, title FROM %s) RHS
        USING(asin)
        ORDER BY avg_helpful_rate DESC
        LIMIT %s
        """ % ("reviews_"+table_name, "meta_"+table_name, num)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()
        out[table_name]['helpful_rate_rank'] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


# search module
# search meta table by asin
@app.route('/<table_name>/search/item/id/<asin>')
def search_meta_asin_one(table_name, asin):

    out = dict()
    out[table_name] = dict()
    sql = "SELECT * FROM %s WHERE asin = '%s'" % ("meta_"+table_name, asin)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]["search_id"] = asin
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search meta table by asin, specifying column
@app.route('/<table_name>/search/item/id/<asin>/<column>')
def search_meta_asin_one_col(table_name, asin, column):

    out = dict()
    out[table_name] = dict()
    sql = "SELECT %s FROM %s WHERE asin = '%s'" % (column, "meta_"+table_name, asin)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchone()
        out[table_name]["search_id"] = asin
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search review table by asin
@app.route('/<table_name>/search/review/id/<asin>')
def search_reviews_asin_one(table_name, asin):

    out = dict()
    out[table_name] = dict()
    sql = \
        """
        SELECT asin, reviewer_ID, reviewer_Name, helpful,
        review_Text, overall, summary, Review_unixTime
        FROM %s WHERE asin = '%s'
        """ % ("reviews_"+table_name, asin)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_id"] = asin
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search review table by asin, specifying column
@app.route('/<table_name>/search/review/id/<asin>/<column>')
def search_reviews_asin_one_col(table_name, asin, column):

    out = dict()
    out[table_name] = dict()
    sql = \
        """
        SELECT %s
        FROM %s WHERE asin = '%s'
        """ % (column, "reviews_"+table_name, asin)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_id"] = asin
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search meta table by title keyword
@app.route('/<table_name>/search/meta/title/<keyword>')
def search_meta_title_one(table_name, keyword):

    out = dict()
    out[table_name] = dict()
    sql = \
        """
        SELECT *
        FROM %s WHERE title ILIKE '%s'
        """ % ("meta_"+table_name, '%'+keyword+'%')
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_keyword"] = keyword
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search meta table by title keyword, specifying column
@app.route('/<table_name>/search/meta/title/<keyword>/<column>')
def search_meta_title_one_col(table_name, keyword, column):

    out = dict()
    out[table_name] = dict()
    if column != 'title':
        column = 'title, '+column
    sql = \
        """
        SELECT %s
        FROM %s WHERE title ILIKE '%s'
        """ % (column, "meta_"+table_name, '%'+keyword+'%')
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_keyword"] = keyword
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)


# search review table by title keyword
@app.route('/<table_name>/search/review/title/<keyword>')
def search_reviews_title_one(table_name, keyword):

    out = dict()
    out[table_name] = dict()
    sql = \
        """
        SELECT * FROM
        (SELECT asin, title FROM %s
        WHERE title ILIKE '%s') LHS
        INNER JOIN
        (SELECT * FROM %s) RHS
        USING(asin)
        """ % ("meta_"+table_name, '%'+keyword+'%', "reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_keyword"] = keyword
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search review table by title keyword, specifying column
@app.route('/<table_name>/search/review/title/<keyword>/<column>')
def search_reviews_title_one_col(table_name, keyword, column):

    out = dict()
    out[table_name] = dict()
    sql = \
        """
        SELECT title, %s FROM
        (SELECT asin, title FROM %s
        WHERE title ILIKE '%s') LHS
        INNER JOIN
        (SELECT * FROM %s) RHS
        USING(asin)
        """ % (column, "meta_"+table_name, '%'+keyword+'%', "reviews_"+table_name)
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs = cur.fetchall()

        out[table_name]["search_keyword"] = keyword
        out[table_name]["search_result"] = rs

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

    return jsonify(out)

# search review table by multiple asin, specifying multiple columns, using post
@app.route('/<table_name>/search/review/id/', methods=["POST","GET"])
def search_reviews_id_multi(table_name):
    all_args = request.get_json(force=True)
    # all_args = {a:b for (a,b) in all_args}
    out = dict()
    out[table_name] = [{}]*len(all_args['id'])
    try:
        if 'asin' not in all_args['column']:
            all_args['column'].append('asin')
        column = ','.join(all_args['column'])
    except:
        column = "reviewer_ID, asin, reviewer_Name, helpful, review_Text, overall, summary, Review_unixTime"
    for i in range(len(all_args['id'])):
        asin = all_args['id'][i]
        sql = \
            """
            SELECT %s
            FROM %s WHERE asin = '%s'
            """ % (column, "reviews_"+table_name, asin)
        try:
            conn = psycopg2.connect(dsn=DB_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rs = cur.fetchall()
            cache = {"search_id":asin, "search_result":rs}
            out[table_name][i] = cache

        except psycopg2.Error as e:
            print e.message
        else:
            cur.close()
            conn.close()

    return jsonify(out)

# search meta table by multiple asin, specifying multiple columns, using post
@app.route('/<table_name>/search/meta/id/', methods=["POST","GET"])
def search_meta_id_multi(table_name):
    all_args = request.get_json(force=True)
    # all_args = {a:b for (a,b) in all_args}
    out = dict()
    out[table_name] = [{}]*len(all_args['id'])
    try:
        if 'asin' not in all_args['column']:
            all_args['column'].append('asin')
        column = ','.join(all_args['column'])
    except:
        column = "*"

    for i in range(len(all_args['id'])):
        asin = all_args['id'][i]
        sql = "SELECT %s FROM %s WHERE asin = '%s'" % (column, "meta_"+table_name, asin)
        try:
            conn = psycopg2.connect(dsn=DB_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rs = cur.fetchall()

            cache = {"search_id":asin, "search_result":rs}
            out[table_name][i] = cache

        except psycopg2.Error as e:
            print e.message
        else:
            cur.close()
            conn.close()

    return jsonify(out)

# search review table by multiple title keyword, specifying multiple columns, using query string
@app.route('/<table_name>/search/review/title/')
def search_reviews_title_multi(table_name):
    all_args = request.args.lists()
    all_args = {a:b for (a,b) in all_args}
    out = dict()
    out[table_name] = [{}]*len(all_args['keyword'])
    try:
        column = ','.join(all_args['column'])
    except:
        column = "reviewer_ID, asin, reviewer_Name, helpful, review_Text, overall, summary, Review_unixTime"
    for i in range(len(all_args['keyword'])):
        keyword = all_args['keyword'][i]
        sql = \
            """
            SELECT title, %s FROM
            (SELECT asin, title FROM %s
            WHERE title ILIKE '%s') LHS
            INNER JOIN
            (SELECT * FROM %s) RHS
            USING(asin)
            """ % (column, "meta_"+table_name, '%'+keyword+'%', "reviews_"+table_name)
        try:
            conn = psycopg2.connect(dsn=DB_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rs = cur.fetchall()
            cache = {"search_keyword":keyword, "search_result":rs}
            out[table_name][i] = cache

        except psycopg2.Error as e:
            print e.message
        else:
            cur.close()
            conn.close()

    return jsonify(out)

# search meta table by multiple title keyword, specifying multiple columns, using query string
@app.route('/<table_name>/search/meta/title/')
def search_meta_title_multi(table_name):
    all_args = request.args.lists()
    all_args = {a:b for (a,b) in all_args}
    out = dict()
    out[table_name] = [{}]*len(all_args['keyword'])
    try:
        if 'title' not in all_args['column']:
            all_args['column'].append('title')
        column = ','.join(all_args['column'])
    except:
        column = "*"
    for i in range(len(all_args['keyword'])):
        keyword = all_args['keyword'][i]
        sql = \
            """
            SELECT %s
            FROM %s WHERE title ILIKE '%s'
            """ % (column, "meta_"+table_name, '%'+keyword+'%')
        try:
            conn = psycopg2.connect(dsn=DB_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)
            rs = cur.fetchall()

            cache = {"search_keyword":keyword, "search_result":rs}
            out[table_name][i] = cache

        except psycopg2.Error as e:
            print e.message
        else:
            cur.close()
            conn.close()

    return jsonify(out)

# @app.route("/test/")
# def hello():
#     all_args = request.args.lists()
#     all_args = {a:b for (a,b) in all_args}
#     print all_args
#     return jsonify(all_args)

if __name__ == "__main__":
    # app.debug = True # only have this on for debugging!
    app.run(host='0.0.0.0') # need this to access from the outside world!
