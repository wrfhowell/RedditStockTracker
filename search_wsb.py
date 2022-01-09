from psaw import PushshiftAPI
import config
import datetime
import psycopg2
import psycopg2.extras
import re

connect = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connect.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()


stocks = {}
for row in rows: 
    stocks['$' + row['symbol']] = row['id']


api = PushshiftAPI()

start_time = int(datetime.datetime(2021, 1, 30).timestamp())

submissions = api.search_submissions(after=start_time,
                                     subreddit='wallstreetbets',
                                     filter=['url','author', 'title', 'subreddit'])
                                     

for submission in submissions:
    words = submission.title.split()
    regex = re.compile(r'(\$[A-Z]{1,6})')
    cashtags = list(set(filter(regex.match, words)))

    if len(cashtags) > 0 & (len(cashtags) <= 5):
        print(cashtags)
        print(submission.title)

        for cashtag in cashtags:
            if cashtag in stocks:
                submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()

                try:
                    cursor.execute("""
                        INSERT INTO ment (dt, stock_id, message, source, url)
                        VALUES (%s, %s, %s, 'wallstreetbets', %s)
                    """, (submitted_time, stocks[cashtag], submission.title, submission.url))

                    connect.commit()
                except Exception as e:
                    print(e)
                    connect.rollback()