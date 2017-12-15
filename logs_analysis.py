import psycopg2
from datetime import datetime

DB_NAME = 'news'


def connect(dbname):
    """ Connect to database with name {dbname}. """

    return psycopg2.connect('dbname=%s' % dbname)


def create_article_views_view(db, cursor):
    cursor.execute('DROP VIEW IF EXISTS article_views;')
    cursor.execute('CREATE VIEW article_views AS '
                   'SELECT author, title, count '
                   'FROM articles JOIN '
                   '(SELECT substring(path from %s) as path_slug, count(*) '
                   'FROM log WHERE path ~ %s AND status = %s '
                   'GROUP BY path) AS views '
                   'ON articles.slug = views.path_slug;',
                   ('/article/(.*)', '/article/*', '200 OK',))
    db.commit()


def create_request_count_view(db, cursor):
    cursor.execute('DROP VIEW IF EXISTS request_count;')
    cursor.execute('CREATE VIEW request_count AS '
                   'SELECT time::date, count(*) as requests '
                   'FROM log '
                   'GROUP BY time::date;')
    db.commit()


def create_error_count_view(db, cursor):
    cursor.execute('DROP VIEW IF EXISTS error_count;')
    cursor.execute('CREATE VIEW error_count AS '
                   'SELECT time::date, count(*) AS errors '
                   'FROM log WHERE status != %s '
                   'GROUP BY time::date;',
                   ('200 OK',))
    db.commit()


def report_popular_articles(cursor, count):
    """ Report the most popular articles. Popularity determined by the number
        of views per article. Result reported in a sorted order with most
        popular article at the top.
    """

    cursor.execute('SELECT title, count '
                   'FROM article_views '
                   'ORDER BY count DESC '
                   'LIMIT %s;',
                   (count,))
    results = cursor.fetchall()

    print '--- Popular Articles ---'
    for x in xrange(0, len(results)):
        article_str = results[x][0]
        views_str = results[x][1]
        print '%s. %s (%s views)' % (x+1, article_str, views_str)
    print '------------------------\n'

    return results


def report_popular_authors(cursor):
    """ Report the most popular article authors. Popularity determined by the
        sum of the number of views of each article written by an author.
        Result reported in a sorted order with most popular author at the top.
    """

    cursor.execute('SELECT name, sum(count) as views '
                   'FROM authors JOIN article_views '
                   'ON authors.id = article_views.author '
                   'GROUP BY authors.id '
                   'ORDER BY views DESC;')
    results = cursor.fetchall()

    print '--- Popular Authors ---'
    for x in xrange(0, len(results)):
        author_str = results[x][0]
        views_str = results[x][1]
        print '%s. %s (%s article views)' % (x+1, author_str, views_str)
    print '-----------------------\n'

    return results


def report_error_days(cursor, percent):
    """ Report the days for which the percent of requests that lead to errors
        exceeds {percent} for the day. Results reported in a sorted order with
        the day with the highest error rate at the top.
    """

    # SELECT time::date, count(*) as success_requests FROM log WHERE status = '200 OK' GROUP BY time::date ORDER BY time::date ASC; # successful requests each day
    # SELECT time::date, count(*) as errors FROM log WHERE status != '200 OK' GROUP BY time::date ORDER BY time::date ASC;  # error requests each day
    # SELECT time::date, count(*) as requests FROM log GROUP BY time::date ORDER BY time::date ASC;   # total requests each day
    # total requests and error requests at once:
    # SELECT request_count.time, requests, errors FROM request_count JOIN (SELECT time::date, count(*) AS errors FROM log WHERE status != '200 OK' GROUP BY time::date) AS error_count ON request_count.time = error_count.time;

    cursor.execute('SELECT request_count.time, '
                   'ROUND((errors::numeric / requests::numeric) * 100, 2) '
                   'AS error_percent '
                   'FROM request_count JOIN error_count '
                   'ON request_count.time = error_count.time '
                   'WHERE '
                   'ROUND((errors::numeric / requests::numeric)*100, 2) > %s '
                   'ORDER BY error_percent DESC;',
                   (percent,))
    results = cursor.fetchall()

    print '--- Daily Request Error Rate > %s%% ---' % percent
    for x in xrange(0, len(results)):
        date_str = datetime.strftime(results[x][0], '%b %d, %Y')
        error_str = results[x][1]
        print '%s. %s (%s%% errors)' % (x+1, date_str, error_str)
    print '---------------------------------------\n'

    return results


if __name__ == '__main__':

    try:
        # Connect to database
        db_connection = connect(DB_NAME)
    except Exception as e:
        print 'Failed to establish connection to "%s" database!' % DB_NAME
        raise e

    # Create a cursor object for queries
    db_cursor = db_connection.cursor()

    # Retrieve and output log data analysis
    create_article_views_view(db_connection, db_cursor)
    report_popular_articles(db_cursor, 3)
    report_popular_authors(db_cursor)

    create_request_count_view(db_connection, db_cursor)
    create_error_count_view(db_connection, db_cursor)
    report_error_days(db_cursor, 1)

    # Close db connection at end to release connection resource
    db_connection.close()
