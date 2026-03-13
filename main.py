"""
SKITCHEN B2B Lead Scraper
Railway + Google Sheets + SerpAPI Edition
"""

import os, time, random, re, json, requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

SERP_API_KEY   = os.environ.get("SERP_API_KEY", "")
SHEETS_CREDS   = os.environ.get("GOOGLE_SHEETS_CREDS", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
PROXY          = os.environ.get("PROXY_URL", "")

INSTAGRAM_HASHTAGS = [
    "dubaigym", "dubaifit", "dubaifitness",
    "dubaicafe", "healthydubai", "dubaiwellness",
    "dubaispa", "dubaiyoga", "crossfitdubai", "healthyfooddubai",
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

AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(AGENTS), "Accept-Language": "en-US,en;q=0.9"}

def delay():
    time.sleep(random.uniform(2.0, 4.5))

def get_proxies():
    return {"http": PROXY, "https": PROXY} if PROXY else None

def get_sheets_client():
    creds_json = json.loads(SHEETS_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def ensure_sheet(spreadsheet, title, hdrs):
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(hdrs))
        ws.append_row(hdrs, value_input_option="RAW")
        ws.freeze(rows=1)
    return ws

def append_leads(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"  ✅ Додано {len(rows)} рядків у '{ws.title}'")
    else:
        print(f"  ℹ️  Немає нових рядків для '{ws.title}'")

def scrape_instagram(hashtag):
    print(f"  📸 Instagram #{hashtag}...")
    try:
        url = f"https://www.instagram.com/explore/tags/{hashtag}/"
        resp = requests.get(url, headers=get_headers(), proxies=get_proxies(), timeout=15)
        usernames = list(set(re.findall(r'"username":"([^"]+)"', resp.text)))[:15]
        delay()
        return usernames
    except Exception as e:
        print(f"    ⚠️ {e}")
        return []

def enrich_instagram(username, source):
    try:
        url = f"https://www.instagram.com/{username}/?__a=1&__d=dis"
        resp = requests.get(url, headers=get_headers(), proxies=get_proxies(), timeout=10)
        user = resp.json().get("graphql", {}).get("user", {})
        bio = user.get("biography", "")
        email_m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", bio)
        phone_m = re.search(r"(\+971|00971)[\s\-]?[0-9]{8,10}", bio)
        return [
            f"@{username}", user.get("full_name", username),
            user.get("category_name", ""),
            user.get("edge_followed_by", {}).get("count", ""),
            email_m.group() if email_m else "",
            phone_m.group() if phone_m else "",
            user.get("external_url", ""), bio[:150], source,
            "✅" if user.get("is_business_account") else "❓",
            "Новий", datetime.today().strftime("%Y-%m-%d"),
        ]
    except Exception:
        return [f"@{username}", username, "", "", "", "", "", "", source, "❓",
                "Новий", datetime.today().strftime("%Y-%m-%d")]

def search_linkedin_serp(query):
    print(f"  💼 LinkedIn: {query[:55]}...")
    if not SERP_API_KEY:
        print("    ⚠️ SERP_API_KEY не вказано")
        return []
    try:
        resp = requests.get("https://serpapi.com/search", params={
            "api_key": SERP_API_KEY,
            "engine": "google",
            "q": query,
            "num": 10,
            "hl": "en",
            "gl": "ae",
        }, timeout=15)
        data = resp.json()
        if "error" in data:
            print(f"    ⚠️ SerpAPI: {data['error']}")
            return []
        results = []
        for item in data.get("organic_results", []):
            link = item.get("link", "")
            if "linkedin.com" not in link:
                continue
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            profile_type = "Company" if "/company/" in link else "Person"
            parts = title.split(" - ")
            name     = parts[0].strip() if parts else title
            position = parts[1].strip() if len(parts) > 1 else ""
            company  = parts[2].strip() if len(parts) > 2 else ""
            results.append([name, position, company, profile_type,
                            link, snippet[:150], query[:60],
                            "Новий", datetime.today().strftime("%Y-%m-%d")])
        print(f"    → {len(results)} результатів")
        delay()
        return results
    except Exception as e:
        print(f"    ⚠️ {e}")
        return []

def run():
    print("=" * 55)
    print("  SKITCHEN Lead Scraper — Railway + SerpAPI")
    print(f"  {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    print("\n📊 Підключення до Google Sheets...")
    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print(f"  ✅ Підключено: '{spreadsheet.title}'")
    except Exception as e:
        print(f"  ❌ Помилка: {e}")
        return

    ws_ig  = ensure_sheet(spreadsheet, "📸 Instagram", [
        "Username", "Повне ім'я", "Категорія", "Followers",
        "Email", "Телефон", "Сайт", "Біографія", "Хештег", "Бізнес?", "Статус", "Дата",
    ])
    ws_li  = ensure_sheet(spreadsheet, "💼 LinkedIn", [
        "Ім'я / Компанія", "Посада", "Компанія", "Тип",
        "LinkedIn URL", "Опис", "Запит", "Статус", "Дата",
    ])
    ws_log = ensure_sheet(spreadsheet, "📋 Лог запусків", [
        "Дата запуску", "Instagram лідів", "LinkedIn лідів", "Всього", "Статус",
    ])

    print("\n📸 INSTAGRAM...\n")
    ig_rows = []
    for hashtag in INSTAGRAM_HASHTAGS:
        usernames = scrape_instagram(hashtag)
        for username in usernames:
            ig_rows.append(enrich_instagram(username, f"#{hashtag}"))
            time.sleep(0.5)
        print(f"    → {len(usernames)} профілів з #{hashtag}")
    append_leads(ws_ig, ig_rows)

    print("\n💼 LINKEDIN (SerpAPI)...\n")
    li_rows = []
    for query in LINKEDIN_QUERIES:
        li_rows.extend(search_linkedin_serp(query))

    seen_urls, unique_li = set(), []
    for row in li_rows:
        if row[4] not in seen_urls:
            seen_urls.add(row[4])
            unique_li.append(row)
    li_rows = unique_li
    append_leads(ws_li, li_rows)

    total = len(ig_rows) + len(li_rows)
    ws_log.append_row([datetime.today().strftime("%Y-%m-%d %H:%M"),
                       len(ig_rows), len(li_rows), total, "✅ Успішно"])

    print(f"\n{'=' * 55}")
    print(f"  ✅ ГОТОВО!")
    print(f"  📸 Instagram: {len(ig_rows)} лідів")
    print(f"  💼 LinkedIn:  {len(li_rows)} лідів")
    print(f"  📊 Всього:    {total} лідів")
    print(f"{'=' * 55}")

if __name__ == "__main__":
    run()
