import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import hashlib
from urllib.parse import urlparse

URL = "https://lenta.ru/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

def normalize_url(url):
    """Нормализуем URL для дедупликации"""
    path = urlparse(url).path
    path = path.rstrip('/')  # удаляем слеш в конце
    path = path.lower()      # нижний регистр
    return path

def make_url_hash(url):
    """Создаём хеш от нормализованного пути"""
    norm = normalize_url(url)
    return hashlib.md5(norm.encode('utf-8')).hexdigest()

def init_db():
    """Создаём базу с url_hash как уникальным ключом"""
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT,              -- оригинальный URL
            url_hash TEXT UNIQUE,  -- хеш для дедупликации
            text TEXT,
            image_url TEXT,
            parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_news(title, url, text, image_url):
    """Сохраняем новость, используя url_hash для уникальности"""
    url_hash = make_url_hash(url)
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO news (title, url, url_hash, text, image_url)
            VALUES (?, ?, ?, ?, ?)
        ''', (title, url, url_hash, text, image_url))
        conn.commit()
    except Exception as e:
        print(f"Ошибка сохранения: {e}")
    finally:
        conn.close()

def parse_article(article_url):
    """Парсим полную статью"""
    try:
        response = requests.get(article_url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Текст
        text_parts = soup.select('div.topic-body p')
        text = "\n".join(p.get_text(strip=True) for p in text_parts)

        # Картинка
        image_tag = soup.select_one('img.topic-cover__image, img[data-src]')
        image_url = None
        if image_tag:
            image_url = image_tag.get('data-src') or image_tag.get('src')
            if image_url and image_url.startswith('/'):
                image_url = "https://lenta.ru" + image_url

        return text, image_url

    except Exception as e:
        return f"Ошибка парсинга статьи: {e}", None

def parse_lenta():
    """Парсим главную и обрабатываем статьи"""
    try:
        response = requests.get(URL, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for item in soup.select('a[href^="/news/"]')[:5]:
            title_tag = item.select_one('h3, span')
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            article_url = "https://lenta.ru" + item['href']

            # Проверка дубля по хешу (без запроса в БД — INSERT OR IGNORE достаточно)
            text, image_url = parse_article(article_url)
            save_news(title, article_url, text, image_url)
            print(f"✅ Сохранено: {title[:50]}...")

            time.sleep(1)

    except Exception as e:
        print(f"Ошибка главной: {e}")

if __name__ == "__main__":
    init_db()
    print("Парсинг Lenta.ru с надёжной дедупликацией...\n")
    parse_lenta()
    print("\nГотово. Данные в news.db")