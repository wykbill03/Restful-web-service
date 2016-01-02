# __author__ = Yikai Wang
import json
import psycopg2, psycopg2.extras
import StringIO
import sys

# DSN location of the AWS - RDS instance
# DB_DSN = "host= dbname= user= password= "

def transform_meta_data(line):
    """
    :param line: one line in the file to be transformed
    :return: list of transformed data to be inserted into the meta table
    """
    # meta data: "asin": text, "title": text, "salesRank": json, "price": real, "also_viewed": list of text,
    # "buy_after_viewing": list of text, "categories": list of text, "description": text

    record=json.loads(line)
    asin = record["asin"]
    try:
        title = record["title"]
    except:
        title = None
    try:
        salesRank = record["salesRank"]
        # use str to convert unicode to string. use json.dumps to replace single quote with double quotes
        salesRank = json.dumps(dict([(str(k), v) for k, v in salesRank.items()]))
    except:
        salesRank = None
    try:
        price = record["price"]
    except:
        price = None
    try:
        also_viewed = record["related"]["also_viewed"]
        also_viewed = json.dumps([str(x) for x in also_viewed])
    except:
        also_viewed = None
    try:
        buy_after_viewing = record["related"]["buy_after_viewing"]
        buy_after_viewing = json.dumps([str(x) for x in buy_after_viewing])
    except:
        buy_after_viewing = None
    try:
        description = record["description"]
    except:
        description = None
    try:
        categories = record["categories"][0]
        categories = json.dumps([str(x) for x in categories])
    except:
        categories = None

    data=[asin, title, salesRank, price, also_viewed, buy_after_viewing, categories, description]
    return data


def transform_review_data(line):
    """
    :param line: one line in the file to be transformed
    :return: list of transformed data to be inserted into the reviews table
    """
    # review data: "reviewerID": text, "asin": text, "reviewerName": text, "helpful": int[], "reviewText": text,
    # "overall": real, "summary": text, "unixReviewTime": timestamp, "reviewTime": time
    key_list = ["reviewerID", "asin", "reviewerName", "helpful", "reviewText", "overall", "summary", "unixReviewTime"]
    data = [0]*len(key_list)
    record=json.loads(line)
    available_keys = record.keys()
    for i in range(len(key_list)):
        if key_list[i] not in available_keys:
            data[i] = None
        else:
            data[i] = record[key_list[i]]

    return data

def drop_table(table_name):
    """
    drops the table 'restaurants' if it exists
    :return:
    """
    try:
       sql = "DROP TABLE IF EXISTS %s;" % table_name
       conn = psycopg2.connect(dsn=DB_DSN)
       cur = conn.cursor()
       cur.execute(sql)
       conn.commit()
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

def create_table(table_name):
    """
    creates a postgres (meta or reviews) table with columns
    :return:
    """
    # "asin": text, "title": text, "salesRank": json, "price": real, "also_viewed": list of text,
    # "buy_after_viewing": list of text, "categories": list of text, "description": text
    # review data: "reviewerID": text, "asin": text, "reviewerName": text, "helpful": int[], "reviewText": text,
    # "overall": real, "summary": text, "unixReviewTime": timestamp, "reviewTime": time
    try:
        if 'meta' in table_name:
            sql = \
               """
               CREATE TABLE %s (
                asin TEXT UNIQUE,
                title TEXT,
                sales_rank JSON,
                price REAL,
                also_viewed TEXT[],
                buy_after_viewing TEXT[],
                categories TEXT[],
                description TEXT
                );
               """ % table_name
        if 'review' in table_name:
            sql = \
               """
               CREATE TABLE %s (
                reviewer_ID TEXT,
                asin TEXT,
                reviewer_Name TEXT,
                helpful INT[],
                review_Text TEXT,
                overall REAL,
                summary TEXT,
                Review_unixTime bigint
                );
               """ % table_name
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()

def insert_data(table_name, filename, start_line = 1, batch = 5000):
    """
    inserts the data using copy_from
    :param: name of table to be inserted into, data file to be inserted, line index to start,
            number of rows in one batch
    :return:
    """
    if 'meta' in filename:
        transform_line = transform_meta_data
    if 'review' in filename:
        transform_line = transform_review_data
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        with open(filename,"rb") as in_file:
            i=0
            to_write_all = ""
            for line in in_file:
                i=i+1
                if i< start_line:
                    continue
                data = transform_line(line)
                # use \f as column separator
                to_write = '\f'.join([str(x).replace("\f"," ") for x in data])
                # formatting string to get around various errors
                to_write = to_write.replace("[","{")
                to_write = to_write.replace("]","}")
                to_write = to_write.replace("\n"," ")
                to_write = to_write.replace("\r"," ")
                to_write = to_write.replace("\\","/")
                to_write = to_write + '\n'
                # stack rows
                to_write_all = to_write_all + to_write

                if i % batch == 0:
                    cpy = StringIO.StringIO()
                    cpy.write(to_write_all)
                    cpy.seek(0)
                    to_write_all = ""
                    cur.copy_from(cpy, table_name, null="None", sep='\f')
                    conn.commit()
                    print "inserted %d rows" % i

            # insert the last block of the file
            cpy = StringIO.StringIO()
            cpy.write(to_write_all)
            cpy.seek(0)

            cur.copy_from(cpy, table_name, null="None", sep='\f')
            conn.commit()
            print "inserted %d rows" % i
    except psycopg2.Error as e:
       print e.message
    else:
       cur.close()
       conn.close()

if __name__ == '__main__':

    META_DATA = ['fixed_meta_Beauty.json', 'fixed_meta_Digital_Music.json', 'fixed_meta_Books.json']
    meta_table = ['meta_Beauty', 'meta_Digital_Music', 'meta_Books']

    REVIEWS_DATA = ['reviews_Beauty.json', 'reviews_Digital_Music.json', 'reviews_Books.json']
    reviews_table = ['reviews_Beauty', 'reviews_Digital_Music', 'reviews_Books']

    for i in range(len(meta_table)):
        # drop the db
        print "dropping table %s" % meta_table[i]
        drop_table(meta_table[i])

        # create the db
        print "creating table %s" % meta_table[i]
        create_table(meta_table[i])

        # insert the data
        print "inserting %s data" % meta_table[i]
        insert_data(meta_table[i], META_DATA[i])

    for i in range(len(reviews_table)):
        # drop the db
        print "dropping table %s" % reviews_table[i]
        drop_table(reviews_table[i])

        # create the db
        print "creating table %s" % reviews_table[i]
        create_table(reviews_table[i])

        # insert the data
        print "inserting %s data" % reviews_table[i]
        insert_data(reviews_table[i], REVIEWS_DATA[i])

