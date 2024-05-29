import requests
import time
import json
import mysql.connector
from mysql.connector import Error

# Параметри підключення до бази даних
DB_HOST = '192.168.77.11'
DB_DATABASE = 'monitoring'
DB_USER = 'prizrakb2'
DB_PASSWORD = '17101995aA'

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


def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        database=DB_DATABASE,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_debug_setting():
    try:
        connection = get_db_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT debug FROM settings LIMIT 1")
            result = cursor.fetchone()
            return result[0] == 1
    except Error as e:
        print(f"Error while fetching debug setting: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def execute_query(cursor, query, values=None, debug=False):
    try:
        if debug:
            print("Executing query:", query)
            print("With values:", values)
        cursor.execute(query, values)
        if debug:
            print(f"Rows affected: {cursor.rowcount}")
            if cursor.lastrowid:
                print(f"Last inserted ID: {cursor.lastrowid}")
    except Error as e:
        print(f"Error while executing query: {e}")
        if debug:
            print("Failed query:", query)
            print("With values:", values)


def get_cities_from_db(debug=False):
    try:
        connection = get_db_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            query = "SELECT city_slug, last_cluster_at, prev_id FROM city_for_scan"
            execute_query(cursor, query, debug=debug)
            cities = cursor.fetchall()
            if debug:
                print("Fetched cities:", cities)
            return cities
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def save_news_to_db(city_slug, news, debug=False):
    try:
        connection = get_db_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            max_news_id = 0
            for item in news:
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
            print(f"News and sources for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Update prev_id in city_for_scan
            if max_news_id > 0:
                update_prev_id_query = """UPDATE city_for_scan SET prev_id = %s WHERE city_slug = %s"""
                update_prev_id_values = (max_news_id, city_slug)
                execute_query(cursor, update_prev_id_query, update_prev_id_values, debug)
                connection.commit()
                print(f"Updated prev_id for {city_slug} to {max_news_id}")

    except Error as e:
        print(f"Error while executing query: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def save_changes_to_db(city_slug, cluster_at, clusters_count, prev_id, debug=False):
    try:
        connection = get_db_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            query = """INSERT INTO chernihiv_changes (city_slug, cluster_at, clusters_count, prev_id) VALUES (%s, %s, %s, %s)"""
            values = (city_slug, cluster_at, clusters_count, prev_id)
            execute_query(cursor, query, values, debug)
            connection.commit()
            print(f"Changes for {city_slug} saved to database at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Error as e:
        print(f"Error while executing query: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def update_last_cluster_at(city_slug, new_cluster_at, debug=False):
    try:
        connection = get_db_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            query = """UPDATE city_for_scan SET last_cluster_at = %s WHERE city_slug = %s"""
            values = (new_cluster_at, city_slug)
            execute_query(cursor, query, values, debug)
            connection.commit()
            print(f"Updated last_cluster_at for {city_slug} to {new_cluster_at}")
    except Error as e:
        print(f"Error while executing query: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def fetch_and_save(city_slug, last_cluster_at, prev_id, debug=False):
    try:
        data = json.dumps({"section_slug": city_slug})
        response = requests.post(url_changes, headers=headers, data=data)
        response.raise_for_status()  # Перевірка на статус код 200
        result = response.json()

        cluster_at = result['cluster_at']  # Зберігаємо як Unix timestamp
        clusters_count = result['clusters_count']

        if cluster_at != last_cluster_at:
            time.sleep(30)
            # Виконати додатковий запит
            data_clusters = json.dumps({"section_slug": city_slug, "prev_id": prev_id})
            response_clusters = requests.post(url_clusters_list, headers=headers, data=data_clusters)
            response_clusters.raise_for_status()  # Перевірка на статус код 200
            result_clusters = response_clusters.json()
            news = result_clusters['data']
            # Зберегти отримані дані
            save_news_to_db(city_slug, news, debug)
            save_changes_to_db(city_slug, cluster_at, clusters_count, prev_id, debug)
            update_last_cluster_at(city_slug, cluster_at, debug)
        else:
            print(f"No changes for {city_slug}")

    except Exception as e:
        print(f"An error occurred for {city_slug}: {e}")


def main():
    while True:
        debug = get_debug_setting()
        cities = get_cities_from_db(debug)
        if debug:
            print("Cities:", cities)
        for city in cities:
            city_slug, last_cluster_at, prev_id = city
            fetch_and_save(city_slug, last_cluster_at, prev_id, debug)
            time.sleep(150)
        time.sleep(300)  # Чекаємо 5 хвилин (300 секунд) перед наступним запитом


if __name__ == "__main__":
    main()
