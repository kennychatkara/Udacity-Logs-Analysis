import psycopg2
from datetime import datetime

DB_NAME = 'news'


def connect(dbname):
    """ Connect to database with name {dbname}. """

    return psycopg2.connect('dbname=%s' % dbname)


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

    if results:
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

    if results:
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

    if results:
        print '--- Daily Request Error Rates > %s%% ---' % percent
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
    try:
        report_popular_articles(db_cursor, 3)
    except psycopg2.DatabaseError:
        print 'Error: Failed to fetch popular articles.\n'
        db_connection.rollback()

    try:
        report_popular_authors(db_cursor)
    except psycopg2.DatabaseError:
        print 'Error: Failed to fetch popular authors.\n'
        db_connection.rollback()

    try:
        report_error_days(db_cursor, 1)
    except psycopg2.DatabaseError:
        print 'Error: Failed to fetch daily request error rates.\n'
        db_connection.rollback()

    # Close db connection at end to release connection resource
    db_connection.close()
