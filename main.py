import requests
import time
import json
import threading
import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Параметри підключення до бази даних
DB_HOST = '192.168.77.11'
DB_DATABASE = 'monitoring'
DB_USER = 'prizrak'
DB_PASSWORD = ''

url_changes = "https://www.ukr.net/api/3/section/changes"
url_clusters_list = "https://www.ukr.net/api/3/section/clusters/list"
headers = {
    "Host": "www.ukr.net",
    "sec-ch-ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "x-content-language": "uk",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "content-type": "application/json; charset=UTF-8",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "x-requested-with": "XMLHttpRequest",
    "sec-ch-ua-platform": "\"Windows\"",
    "origin": "https://www.ukr.net",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "referer": "https://www.ukr.net/",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "priority": "u=1, i"
}

app = Flask(__name__)

debug_messages = []
status = {"running": True}


def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
            connection_timeout=10  # Додаємо тайм-аут для підключення до бази даних
        )
        if connection.is_connected():
            log_debug("Successfully connected to the database")
        return connection
    except Error as e:
        log_debug(f"Error while connecting to MySQL: {e}")
        return None


def get_debug_setting():
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT debug FROM settings LIMIT 1")
            result = cursor.fetchone()
            return result[0] == 1
    except Error as e:
        log_debug(f"Error while fetching debug setting: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def execute_query(cursor, query, values=None, debug=False):
    try:
        if debug:
            log_debug(f"Executing query: {query}")
            log_debug(f"With values: {values}")
        cursor.execute(query, values)
        if debug:
            log_debug(f"Rows affected: {cursor.rowcount}")
            if cursor.lastrowid:
                log_debug(f"Last inserted ID: {cursor.lastrowid}")
    except Error as e:
        log_debug(f"Error while executing query: {e}")
        if debug:
            log_debug(f"Failed query: {query}")
            log_debug(f"With values: {values}")


def get_cities_from_db(debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            query = "SELECT city_slug, last_cluster_at, prev_id FROM city_for_scan"
            execute_query(cursor, query, debug=debug)
            cities = cursor.fetchall()
            if debug:
                log_debug(f"Fetched cities: {cities}")
            return cities
    except Error as e:
        log_debug(f"Error while connecting to MySQL: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def save_news_to_db(city_slug, news, prev_id, debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            max_news_id = prev_id
            log_debug(f"maxid= {max_news_id}")
            # max_news_id = 0
            for item in news:
                search_news_query = """ SELECT 't1' as typer, COUNT(*) as ccc FROM city_news WHERE news_id=%s UNION SELECT 't2' as typer, COUNT(*) as ccc FROM city_news WHERE news_id=%s AND city_slug=%s AND created_at=%s AND title=%s and link=%s"""
                search_news_values = (
                    item['id'],
                    item['id'],
                    city_slug,
                    item['created_at'],
                    item['title'],
                    item['link']
                )
                execute_query(cursor, search_news_query, search_news_values, debug)
                cn_result = cursor.fetchall()
                if debug:
                    log_debug(f"Checked news stats: {cn_result}")
                tmp_count_all = cn_result[0][1]
                tmp_count_curr = cn_result[1][1]
                if tmp_count_all == 0:
                    log_debug(f"NEED INSERT NEWS")
                    news_query = """INSERT INTO city_news (news_id, city_slug, created_at, title, link)
                                                    VALUES (%s, %s, %s, %s, %s)"""
                    news_values = (
                        item['id'],
                        city_slug,
                        item['created_at'],
                        item['title'],
                        item['link']
                    )
                    execute_query(cursor, news_query, news_values, debug)
                elif tmp_count_all != tmp_count_curr:
                    log_debug(f"NEED UPDATE NEWS")
                    news_query = """UPDATE city_news SET city_slug=%s, created_at=%s, title=%s, link=%s where news_id=%s LIMIT 1"""
                    news_values = (
                        city_slug,
                        item['created_at'],
                        item['title'],
                        item['link'],
                        item['id']
                    )
                    execute_query(cursor, news_query, news_values, debug)
                else:
                    log_debug(f"NEED IGNORE NEWS")

                if item['id'] > max_news_id:
                    max_news_id = item['id']

                if 'source' in item:
                    for source in item['source']:
                        article = source.get('article', {})
                        search_source_query = """ SELECT 't1' as typer, COUNT(*) as ccc FROM news_sources WHERE source_id=%s AND news_id=%s 
                        UNION
                        SELECT 't2' as typer, COUNT(*) as ccc FROM news_sources WHERE
                         news_id=%s AND source_id=%s AND source_title=%s AND source_link=%s AND source_link_original=%s AND  
                                        article_id=%s AND article_created_at=%s AND article_title=%s AND article_link=%s AND article_link_original=%s"""
                        search_source_values = (
                            source['id'],
                            item['id'],
                            item['id'],
                            source['id'],
                            source['title'],
                            source['link'],
                            source.get('link_original'),
                            article.get('id'),
                            article.get('created_at'),
                            article.get('title'),
                            article.get('link'),
                            article.get('link_original')
                        )
                        execute_query(cursor, search_source_query, search_source_values, debug)
                        cs_result = cursor.fetchall()
                        if debug:
                            log_debug(f"Checked source stats: {cs_result}")
                        tmp_scount_all = cs_result[0][1]
                        tmp_scount_curr = cs_result[1][1]
                        if tmp_scount_all == 0:
                            log_debug(f"NEED INSERT SOURCE")
                            source_query = """INSERT INTO news_sources (news_id, source_id, source_title, source_link, source_link_original, 
                                                                    article_id, article_created_at, article_title, article_link, article_link_original)
                                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                            source_values = (
                                item['id'],
                                source['id'],
                                source['title'],
                                source['link'],
                                source.get('link_original'),
                                article.get('id'),
                                article.get('created_at'),
                                article.get('title'),
                                article.get('link'),
                                article.get('link_original')
                            )
                            execute_query(cursor, source_query, source_values, debug)
                        elif tmp_scount_all != tmp_scount_curr:
                            log_debug(f"NEED UPDATE SOURCE")
                            source_query = """UPDATE news_sources set source_title=%s, source_link=%s, source_link_original=%s, 
                                                                    article_id=%s, article_created_at=%s, article_title=%s, article_link=%s, article_link_original=%s 
                                                                    WHERE news_id=%s AND source_id=%s LIMIT 1"""
                            source_values = (
                                source['title'],
                                source['link'],
                                source.get('link_original'),
                                article.get('id'),
                                article.get('created_at'),
                                article.get('title'),
                                article.get('link'),
                                article.get('link_original'),
                                item['id'],
                                source['id']

                            )
                            execute_query(cursor, source_query, source_values, debug)
                        else:
                            log_debug(f"NEED IGNORE SOURCE")
                else:
                    log_debug(f"NO SOURCE TRY TO GET CLUSTERS")

            connection.commit()
            log_debug(f"News and sources for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Update prev_id in city_for_scan
            if max_news_id > 0:
                update_prev_id_query = """UPDATE city_for_scan SET prev_id = %s WHERE city_slug = %s"""
                update_prev_id_values = (max_news_id, city_slug)
                execute_query(cursor, update_prev_id_query, update_prev_id_values, debug)
                connection.commit()
                log_debug(f"Updated prev_id for {city_slug} to {max_news_id}")

    except Error as e:
        log_debug(f"Error while executing query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def save_changes_to_db(city_slug, cluster_at, clusters_count, prev_id, debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            query = """INSERT INTO chernihiv_changes (city_slug, cluster_at, clusters_count, prev_id) VALUES (%s, %s, %s, %s)"""
            values = (city_slug, cluster_at, clusters_count, prev_id)
            execute_query(cursor, query, values, debug)
            connection.commit()
            log_debug(f"Changes for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Error as e:
        log_debug(f"Error while executing query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")

def save_news_to_db(city_slug, news,prev_id, debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            max_news_id = prev_id
            log_debug(f"maxid= {max_news_id}")
            # max_news_id = 0
            for item in news:
                search_news_query = """ SELECT 't1' as typer, COUNT(*) as ccc FROM city_news WHERE news_id=%s UNION SELECT 't2' as typer, COUNT(*) as ccc FROM city_news WHERE news_id=%s AND city_slug=%s AND created_at=%s AND title=%s and link=%s"""
                search_news_values = (
                    item['id'],
                    item['id'],
                    city_slug,
                    item['created_at'],
                    item['title'],
                    item['link']
                )
                execute_query(cursor, search_news_query, search_news_values, debug)
                cn_result = cursor.fetchall()
                if debug:
                    log_debug(f"Checked news stats: {cn_result}")
                tmp_count_all = cn_result[0][1]
                tmp_count_curr = cn_result[1][1]
                if tmp_count_all == 0:
                    log_debug(f"NEED INSERT")
                    news_query = """INSERT INTO city_news (news_id, city_slug, created_at, title, link)
                                                    VALUES (%s, %s, %s, %s, %s)"""
                    news_values = (
                        item['id'],
                        city_slug,
                        item['created_at'],
                        item['title'],
                        item['link']
                    )
                    execute_query(cursor, news_query, news_values, debug)
                elif tmp_count_all != tmp_count_curr:
                    log_debug(f"NEDD UPDATE")
                    news_query = """UPDATE city_news SET city_slug=%s, created_at=%s, title=%s, link=%s where news_id=%s LIMIT 1"""
                    news_values = (
                        city_slug,
                        item['created_at'],
                        item['title'],
                        item['link'],
                        item['id']
                    )
                    execute_query(cursor, news_query, news_values, debug)
                else:
                    log_debug(f"NEED IGNORE")

                if item['id'] > max_news_id:
                    max_news_id = item['id']

                if 'source' in item:
                    for source in item['source']:
                        article = source.get('article', {})
                        source_query = """INSERT INTO news_sources (news_id, source_id, source_title, source_link, source_link_original, 
                                        article_id, article_created_at, article_title, article_link, article_link_original)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        source_values = (
                            item['id'],
                            source['id'],
                            source['title'],
                            source['link'],
                            source.get('link_original'),
                            article.get('id'),
                            article.get('created_at'),
                            article.get('title'),
                            article.get('link'),
                            article.get('link_original')
                        )
                        execute_query(cursor, source_query, source_values, debug)
            connection.commit()
            log_debug(f"News and sources for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Update prev_id in city_for_scan
            if max_news_id > 0:
                update_prev_id_query = """UPDATE city_for_scan SET prev_id = %s WHERE city_slug = %s"""
                update_prev_id_values = (max_news_id, city_slug)
                execute_query(cursor, update_prev_id_query, update_prev_id_values, debug)
                connection.commit()
                log_debug(f"Updated prev_id for {city_slug} to {max_news_id}")

    except Error as e:
        log_debug(f"Error while executing query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def save_changes_to_db(city_slug, cluster_at, clusters_count, prev_id, debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            query = """INSERT INTO chernihiv_changes (city_slug, cluster_at, clusters_count, prev_id) VALUES (%s, %s, %s, %s)"""
            values = (city_slug, cluster_at, clusters_count, prev_id)
            execute_query(cursor, query, values, debug)
            connection.commit()
            log_debug(f"Changes for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Error as e:
        log_debug(f"Error while executing query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def update_last_cluster_at(city_slug, new_cluster_at, debug=False):
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            cursor = connection.cursor()
            query = """UPDATE city_for_scan SET last_cluster_at = %s WHERE city_slug = %s"""
            values = (new_cluster_at, city_slug)
            execute_query(cursor, query, values, debug)
            connection.commit()
            log_debug(f"Updated last_cluster_at for {city_slug} to {new_cluster_at}")
    except Error as e:
        log_debug(f"Error while executing query: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            log_debug("MySQL connection is closed")


def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def fetch_and_save(city_slug, last_cluster_at, prev_id ,its_sec=False, debug=False):
    try:
        data = json.dumps({"section_slug": city_slug})
        session = requests_retry_session()
        response = session.post(url_changes, headers=headers, data=data, timeout=30)
        response.raise_for_status()  # Перевірка на статус код 200
        result = response.json()

        cluster_at = result['cluster_at']  # Зберігаємо як Unix timestamp
        clusters_count = result['clusters_count']

        if cluster_at != last_cluster_at:
            # Виконати додатковий запит
            if not its_sec:
                log_debug(f"its_sec = TRUE")
                data_clusters = json.dumps({"section_slug": city_slug, "prev_id": prev_id})
            else:
                log_debug(f"its_sec = FALSE")
                data_clusters = json.dumps({"section_slug": city_slug})
            log_debug(f"data_cluster={data_clusters}")
            response_clusters = session.post(url_clusters_list, headers=headers, data=data_clusters, timeout=120)
            response_clusters.raise_for_status()  # Перевірка на статус код 200
            result_clusters = response_clusters.json()
            news = result_clusters['data']
            if debug:
                log_debug(f"RESULT OF API GET = {result_clusters}")
            # Зберегти отримані дані
            # save_news_to_db(city_slug, news, prev_id, debug)
            save_news_to_db(city_slug, news, prev_id, debug)
            save_changes_to_db(city_slug, cluster_at, clusters_count, prev_id, debug)
            if its_sec:
                update_last_cluster_at(city_slug, cluster_at, debug)
        else:
            log_debug(f"No changes for {city_slug}")

    except requests.exceptions.RequestException as e:
        log_debug(f"An error occurred during HTTP request for {city_slug}: {e}")
    except Exception as e:
        log_debug(f"An error occurred for {city_slug}: {e}")


def log_debug(message):
    global debug_messages
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    debug_message = f"[{timestamp}] {message}"
    debug_messages.append(debug_message)
    print(debug_message)
    # Зберігаємо тільки останні 100 повідомлень
    if len(debug_messages) > 100:
        debug_messages = debug_messages[-100:]


@app.route('/status')
def status_route():
    return jsonify(status)


@app.route('/debug')
def debug_route():
    return jsonify(debug_messages[-10000:])  # Показуємо останні 10 повідомлень


def main():
    while True:
        status["running"] = True
        debug = get_debug_setting()
        cities = get_cities_from_db(debug)
        if debug:
            log_debug(f"Cities: {cities}")
        for city in cities:
            city_slug, last_cluster_at, prev_id = city
            fetch_and_save(city_slug, last_cluster_at, prev_id,False, debug)
            time.sleep(5)
            fetch_and_save(city_slug, last_cluster_at, prev_id,True, debug)
            time.sleep(150)
        time.sleep(300)  # Чекаємо 5 хвилин (300 секунд) перед наступним запитом


if __name__ == "__main__":
    # Запускаємо Flask сервер у окремому потоці
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000))
    flask_thread.start()

    # Запускаємо основний процес
    main()
