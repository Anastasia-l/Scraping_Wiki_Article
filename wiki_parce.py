from prefect import flow, task, get_run_logger
from prefect.tasks import NO_CACHE
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
import difflib
import os
import json
import sqlite3
from urllib.parse import quote


def create_driver():
    """Создает и возвращает экземпляр браузера"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


@task(retries=2, retry_delay_seconds=120, timeout_seconds=300)
def init_db():
    """Инициализирует базу данных SQLite"""
    logger = get_run_logger()
    logger.info("Инициализация базы данных...")
    with sqlite3.connect("wiki_history.db") as conn:
        c = conn.cursor()
        c.execute('''
                CREATE TABLE IF NOT EXISTS wiki_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    char_count INTEGER,
                    content TEXT,
                    last_edit TEXT,
                    editor TEXT,
                    links TEXT,
                    timestamp DATETIME,
                    diff TEXT,
                    total_views INTEGER,
                    average_daily_views INTEGER
                )
''')
        conn.commit()


@task(retries=2, retry_delay_seconds=60, cache_policy=NO_CACHE)
def get_last_editor(driver, url):
    """Парсит автора последней правки из истории страницы"""
    logger = get_run_logger()
    try:
        history_url = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "ca-history"))
        )
        driver.execute_script("arguments[0].scrollIntoView();", history_url)
        history_url.click()

        # Ждем загрузку истории
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "mw-changeslist-date"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        last_edit = soup.find("a", class_="mw-userlink")
        return last_edit.text.strip() if last_edit else "Unknown"
    except Exception as e:
        logger.error(f"Ошибка при получении редактора: {e}")
        return "Unknown"
    finally:
        # Возвращаемся на исходную страницу
        driver.get(url)
        driver.delete_all_cookies()
        time.sleep(random.uniform(3, 9))
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "firstHeading"))
        )


@task(retries=2, retry_delay_seconds=70, cache_policy=NO_CACHE)
def scrape_wiki(driver, url):
    """Основная логика парсера"""
    logger = get_run_logger()
    logger.info("Запуск парсера...")

    try:
        driver.get(url)  # открывает страницу
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "firstHeading"))
        )

        # Парсинг HTML
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Название статьи
        title = soup.find("h1", {"id": "firstHeading"}).text.strip()  # Ищет заголовок статьи

        # Основной текст статьи
        content = soup.find("div", {"id": "mw-content-text"}).text  # блок с текстом статьи
        char_count = len(content)

        # Все ссылки
        links = []
        for a in soup.find_all("a", href=True):
            link_type = "internal" if a["href"].startswith("/wiki/") else "external"
            links.append({
                "url": a["href"],
                "text": a.text.strip(),
                "type": link_type
            })

        # Дата последней правки
        last_edit = soup.find("li", {"id": "footer-info-lastmod"})
        last_edit = last_edit.text if last_edit else "N/A"

        # Получаем статистику просмотров
        view_stats = get_page_views(driver, title)

        return {
            "title": title,
            "char_count": char_count,
            "content": content,
            "last_edit": last_edit,
            "links": links,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "view_stats": view_stats
        }

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise ValueError(f"Ошибка парсинга: {e}")


@task
def compare_text(old_text, new_text):
    """Находит различия между текстами"""
    logger = get_run_logger()
    logger.info("Сравнение текстов...")
    diff = difflib.unified_diff(
        old_text.splitlines(),
        new_text.splitlines(),
        lineterm=''
    )
    return '\n'.join(diff)


@task
def track_links(old_links, new_links):
    """Находит новые и удаленные ссылки"""
    logger = get_run_logger()
    logger.info("Анализ ссылок...")
    old_urls = {link["url"] for link in old_links}
    new_urls = {link["url"] for link in new_links}

    added = [link for link in new_links if link["url"] not in old_urls]
    removed = [link for link in old_links if link["url"] not in new_urls]
    return {"added": added, "removed": removed}


@task(retries=2, cache_policy=NO_CACHE)
def get_page_views(driver, title):
    """Получаем статистику просмотров статьи"""
    logger = get_run_logger()
    logger.info("Получение статистики просмотров...")
    original_window = driver.current_window_handle
    try:
        driver.switch_to.new_window("tab")

        formatted_title = quote(title.replace(" ", "_"), safe='()_')
        stats_url = f"https://pageviews.wmcloud.org/?project=en.wikipedia.org&platform=all-access&agent=user&redirects=0&range=latest-20&pages={formatted_title}"

        driver.get(stats_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "linear-legend--counts")))

        time.sleep(random.uniform(4, 8))

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Парсим общую статистику
        stats_blocks = soup.find_all("div", class_="linear-legend--counts")

        # Инициализируем значения по умолчанию
        total_views = 0
        average_daily_views = 0

        # Обрабатываем блоки
        for block in stats_blocks:
            label = block.get_text(strip=True, separator=" ")

            if "Просмотры страниц:" in label:
                value_element = block.find("span", class_="pull-right")
                if value_element:
                    total_views = int(''.join(filter(str.isdigit, value_element.text)))

            elif "Ежедневные:" in label:
                value_element = block.find("span", class_="pull-right")
                if value_element:
                    average_daily_views = int(''.join(filter(str.isdigit, value_element.text)))
                if not value_element:
                    continue

        # Закрываем вкладку статистики
        driver.close()
        driver.switch_to.window(original_window)
        driver.delete_all_cookies()

        return {
            "total_views": total_views,
            "average_daily_views": average_daily_views

        }

    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(original_window)
        return {"total_views": 0, "average_daily_views": 0}


@task
def save_to_sqlite(data):
    """Сохраняет данные в SQLite"""
    logger = get_run_logger()
    logger.info("Сохранение в базу данных...")
    conn = sqlite3.connect("wiki_history.db", timeout=30)
    c = conn.cursor()

    links_json = json.dumps(data['links'], ensure_ascii=False)
    diff_json = json.dumps(data['diff'], ensure_ascii=False)

    c.execute('''
              INSERT INTO wiki_history
              (title, char_count, content, last_edit, editor, links, timestamp, diff, total_views, average_daily_views)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

              ''', (
        data['title'],
        data['char_count'],
        data['content'],
        data['last_edit'],
        data['editor'],
        links_json,
        data['timestamp'],
        diff_json,
        data['view_stats']['total_views'],
        data['view_stats']['average_daily_views']

    ))

    conn.commit()
    conn.close()
    logger.info("Данные успешно сохранены")


@task
def get_last_entry():
    """Получает последнюю запись из базы данных"""
    logger = get_run_logger()
    logger.info("Поиск последней записи...")
    if not os.path.exists("wiki_history.db"):
        return None

    conn = sqlite3.connect("wiki_history.db")
    c = conn.cursor()
    c.execute('SELECT * FROM wiki_history ORDER BY id DESC LIMIT 1')
    row = c.fetchone()
    conn.close()

    if not row:
        return None

    return {
        'title': row[1],
        'char_count': row[2],
        'content': row[3],
        'last_edit': row[4],
        'editor': row[5],
        'links': json.loads(row[6]),
        'timestamp': row[7],
        'diff': json.loads(row[8]) if row[8] else None,
        'total_views': row[9],
        'average_daily_views': row[10]
    }


@flow(
    name="Wikipedia Monitoring",
    timeout_seconds=1200
)
def main_flow():
    driver = None
    logger = get_run_logger()
    try:
        driver = create_driver()
        init_db()  # Инициализирует базу при запуске
        url = "https://en.wikipedia.org/wiki/Python_(programming_language)"

        # Собираем текущие данные
        current_data = scrape_wiki(driver, url)
        if not current_data:
            raise ValueError("Не удалось собрать данные. Выход")
        editor = get_last_editor(driver, url)
        current_data["editor"] = editor
        last_data = get_last_entry()

        if last_data:
            text_diff = compare_text(last_data["content"], current_data["content"])
            links_diff = track_links(last_data["links"], current_data["links"])
            current_data["diff"] = {
                "text": text_diff,
                "links": links_diff
            }
        else:
            current_data["diff"] = {'text': "Первая версия", 'links': []}

        # Сохраняем
        save_to_sqlite(current_data)
        logger.info("Данные сохранены!")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        if driver:
            driver.delete_all_cookies()
            driver.quit()


if __name__ == "__main__":
    main_flow.serve(
        name="wiki-monitor",
        tags=["education"],
        description="Web scraping once a day",
        interval=timedelta(hours=24)
    )
