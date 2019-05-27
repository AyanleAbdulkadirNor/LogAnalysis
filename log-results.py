# !/usr/bin/env python
""" Project Log Analysis"""

import psycopg2

DATABASE = "news"
queries = [
    {
        "title": "Q1: What are the most popular three articles of all time?",
        "query": "select articles.title, count(*) as views "
                 "from articles,log WHERE log.path = '/article/' || articles.slug "
                 "group by articles.title, log.path "
                 "order by views desc limit 3;",
        "result": ""
    },
    {
        "title": "Q2: Who are the most popular article authors of all time?",
        "query": "select authors.name, count(*) as views "
                 "from articles, authors, log "
                 "where articles.author = authors.id "
                 "and log.path like concat('%', articles.slug, '%') and "
                 "log.status like '%200%' group by authors.name "
                 "order by views desc;",
        "result": ""
    },
    {
        "title":
            "Q3: On which days did more than 1% of requests lead to errors?",
        "query": "select day, percentage_error_request "
                 "from ( select day, round((sum(requests)/(select count(*) "
                 "from log where "
                 "substring(cast(log.time as text), 0, 11) = day) * 100), 2) "
                 "as percentage_error_request from "
                 "(select substring(cast(log.time as text), 0, 11) "
                 "as day, count(*) as requests from log "
                 "WHERE status != '200 OK' group by day) "
                 "as percentage_requests group by day "
                 "order by percentage_error_request desc) "
                 "as percentage_day_query "
                 "where percentage_error_request >= 1.00;",
        "result": ""
    }
] 

def connect_to_database():
    """ Connects to news data and returns connection object """
    try:
        db = psycopg2.connect(host="localhost",database="news", user="postgres", password="123" , port="5432")
        return db
    except:
        return None


def get_query_result(db, query):
    """ executes the query and returns the result """
    c = db.cursor()
    c.execute(query)
    results = c.fetchall()
    return results


def run_queries():
    """ runs the queries """
    db = connect_to_database()
    if db:
        for query_object in queries:
            result = get_query_result(db, query_object["query"])
            query_object["result"] = result
        db.close()
    else:
        print("Errors connecting to Database")


def print_query_results():
    """ prints the query results """
    for i, query_object in enumerate(queries):
        query_results = query_object["result"]
        print('\n')
        print(query_object["title"])
        if i == 2:
            for result_values in query_results:
                print('\t %s - %s%% errors' % (result_values[0],
                                               str(result_values[1])))
        else:
            for index, results in enumerate(query_results):
                print('\t %d - %s \t - %s views' % (index+1,
                                                    results[0],
                                                    str(results[1])))


if __name__ == "__main__":
    run_queries()
    print_query_results()