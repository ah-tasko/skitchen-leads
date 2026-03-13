"""
SKITCHEN B2B Lead Scraper
Railway + Google Sheets Edition
=================================
Запускається автоматично за розкладом на Railway.
Результати зберігаються в Google Sheets.
"""

import os
import time
import random
import re
import json
import requests
from datetime import datetime
from urllib.parse import quote_plus
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# НАЛАШТУВАННЯ — беруться з Railway Environment Variables
# ============================================================
GOOGLE_API_KEY   = os.environ.get("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID    = os.environ.get("GOOGLE_CSE_ID", "")
SHEETS_CREDS     = os.environ.get("GOOGLE_SHEETS_CREDS", "")   # JSON рядок
SPREADSHEET_ID   = os.environ.get("SPREADSHEET_ID", "")
APIFY_TOKEN      = os.environ.get("APIFY_TOKEN", "")
PROXY            = os.environ.get("PROXY_URL", "")

# ============================================================
# ПОШУКОВІ ЗАПИТИ
# ============================================================
INSTAGRAM_HASHTAGS = [
    "dubaigym", "dubaifit", "dubaifitness",
    "dubaicafe", "healthydubai", "dubaiwellness",
    "dubaispa", "dubaiyoga", "crossfitdubai",
    "healthyfooddubai",
]

LINKEDIN_QUERIES = [
    'site:linkedin.com/in "gym owner" "Dubai"',
    'site:linkedin.com/in "fitness manager" "Dubai"',
    'site:linkedin.com/in "F&B manager" "Dubai"',
    'site:linkedin.com/in "cafe owner" "Dubai"',
    'site:linkedin.com/in "wellness manager" "Dubai"',
    'site:linkedin.com/in "hotel manager" "Dubai"',
    'site:linkedin.com/in "procurement manager" "Dubai"',
    'site:linkedin.com/company "gym" "Dubai"',
    'site:linkedin.com/company "wellness" "Dubai"',
    'site:linkedin.com/company "specialty coffee" "Dubai"',
]

# ============================================================
# HELPERS
# ============================================================
AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
]

def headers():
    return {"User-Agent": random.choice(AGENTS), "Accept-Language": "en-US,en;q=0.9"}

def delay():
    time.sleep(random.uniform(2.0, 4.5))

def proxies():
    return {"http": PROXY, "https": PROXY} if PROXY else None

# ============================================================
# GOOGLE SHEETS
# ============================================================
def get_sheets_client():
    """Підключення до Google Sheets через Service Account."""
    creds_json = json.loads(SHEETS_CREDS)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def ensure_sheet(spreadsheet, title: str, headers: list):
    """Створити вкладку якщо не існує, повернути worksheet."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(headers))
        ws.append_row(headers, value_input_option="RAW")
        # Заморозити перший рядок
        ws.freeze(rows=1)
    return ws

def append_leads(ws, rows: list):
    """Додати ліди в таблицю пакетно."""
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"  ✅ Додано {len(rows)} рядків у '{ws.title}'")

# ============================================================
# INSTAGRAM
# ============================================================
def scrape_instagram(hashtag: str) -> list:
    print(f"  📸 Instagram #{hashtag}...")
    results = []
    url = f"https://www.instagram.com/explore/tags/{hashtag}/"
    try:
        resp = requests.get(url, headers=headers(), proxies=proxies(), timeout=15)
        usernames = re.findall(r'"username":"([^"]+)"', resp.text)
        for username in list(set(usernames))[:15]:
            results.append(username)
        delay()
    except Exception as e:
        print(f"    ⚠️ {e}")
    return results

def enrich_instagram(username: str, source: str) -> list:
    """Повертає рядок для Google Sheets."""
    url = f"https://www.instagram.com/{username}/?__a=1&__d=dis"
    try:
        resp = requests.get(url, headers=headers(), proxies=proxies(), timeout=10)
        user = resp.json().get("graphql", {}).get("user", {})
        bio = user.get("biography", "")
        email_m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", bio)
        phone_m = re.search(r"(\+971|00971)[\s\-]?[0-9]{8,10}", bio)
        return [
            f"@{username}",
            user.get("full_name", username),
            user.get("category_name", ""),
            user.get("edge_followed_by", {}).get("count", ""),
            email_m.group() if email_m else "",
            phone_m.group() if phone_m else "",
            user.get("external_url", ""),
            bio[:150],
            source,
            "✅" if user.get("is_business_account") else "❓",
            "Новий",
            datetime.today().strftime("%Y-%m-%d"),
        ]
    except Exception:
        return [f"@{username}", username, "", "", "", "", "", "", source, "❓", "Новий",
                datetime.today().strftime("%Y-%m-%d")]

# ============================================================
# LINKEDIN
# ============================================================
def search_linkedin(query: str) -> list:
    print(f"  💼 LinkedIn: {query[:55]}...")
    results = []

    if not GOOGLE_API_KEY:
        print("    ⚠️ GOOGLE_API_KEY не вказано — пропускаємо LinkedIn")
        return []

    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": query, "num": 10}
    try:
        resp = requests.get(url, params=params, timeout=10)
        for item in resp.json().get("items", []):
            link = item.get("link", "")
            if "linkedin.com" not in link:
                continue
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            profile_type = "Company" if "/company/" in link else "Person"
            parts = title.split(" - ")
            name     = parts[0].strip() if len(parts) > 0 else title
            position = parts[1].strip() if len(parts) > 1 else ""
            company  = parts[2].strip() if len(parts) > 2 else ""
            results.append([
                name, position, company, profile_type,
                link, snippet[:150], query[:60],
                "Новий", datetime.today().strftime("%Y-%m-%d"),
            ])
        delay()
    except Exception as e:
        print(f"    ⚠️ {e}")
    return results

# ============================================================
# ГОЛОВНА ФУНКЦІЯ
# ============================================================
def run():
    print("=" * 55)
    print("  SKITCHEN Lead Scraper — Railway Edition")
    print(f"  {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    # ── Підключення до Google Sheets ───────────────────────
    print("\n📊 Підключення до Google Sheets...")
    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print(f"  ✅ Підключено: '{spreadsheet.title}'")
    except Exception as e:
        print(f"  ❌ Помилка підключення: {e}")
        return

    # Створити/відкрити вкладки
    ws_ig = ensure_sheet(spreadsheet, "📸 Instagram", [
        "Username", "Повне ім'я", "Категорія", "Followers",
        "Email", "Телефон", "Сайт", "Біографія",
        "Хештег", "Бізнес?", "Статус", "Дата",
    ])
    ws_li = ensure_sheet(spreadsheet, "💼 LinkedIn", [
        "Ім'я / Компанія", "Посада", "Компанія", "Тип",
        "LinkedIn URL", "Опис", "Запит", "Статус", "Дата",
    ])
    ws_log = ensure_sheet(spreadsheet, "📋 Лог запусків", [
        "Дата запуску", "Instagram лідів", "LinkedIn лідів", "Всього", "Статус",
    ])

    # ── Instagram ──────────────────────────────────────────
    print("\n📸 INSTAGRAM...\n")
    ig_rows = []
    for hashtag in INSTAGRAM_HASHTAGS:
        usernames = scrape_instagram(hashtag)
        for username in usernames:
            row = enrich_instagram(username, f"#{hashtag}")
            ig_rows.append(row)
            time.sleep(0.5)
        print(f"    → {len(usernames)} профілів з #{hashtag}")

    append_leads(ws_ig, ig_rows)

    # ── LinkedIn ───────────────────────────────────────────
    print("\n💼 LINKEDIN...\n")
    li_rows = []
    for query in LINKEDIN_QUERIES:
        rows = search_linkedin(query)
        li_rows.extend(rows)
        print(f"    → {len(rows)} результатів")

    # Дедублікація за URL
    seen = set()
    unique_li = []
    for row in li_rows:
        url = row[4]
        if url not in seen:
            seen.add(url)
            unique_li.append(row)
    li_rows = unique_li

    append_leads(ws_li, li_rows)

    # ── Лог ────────────────────────────────────────────────
    total = len(ig_rows) + len(li_rows)
    ws_log.append_row([
        datetime.today().strftime("%Y-%m-%d %H:%M"),
        len(ig_rows),
        len(li_rows),
        total,
        "✅ Успішно",
    ])

    print(f"\n{'=' * 55}")
    print(f"  ✅ ГОТОВО!")
    print(f"  📸 Instagram: {len(ig_rows)} лідів")
    print(f"  💼 LinkedIn:  {len(li_rows)} лідів")
    print(f"  📊 Всього:    {total} лідів")
    print(f"{'=' * 55}")

if __name__ == "__main__":
    run()
