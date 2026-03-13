"""
SKITCHEN B2B Lead Scraper v3
================================
Пайплайн:
  1. Google Maps API → знаходить gym/cafe/wellness у Дубаї
  2. SerpAPI → шукає їх Instagram акаунт за назвою
  3. Apify → витягує деталі Instagram бізнес-профілю
  4. SerpAPI → LinkedIn пошук осіб для outreach
  5. Google Sheets → зберігає всі ліди
"""

import os, time, random, re, json, requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# ЗМІННІ СЕРЕДОВИЩА (Railway)
# ============================================================
GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_KEY", "")
SERP_API_KEY    = os.environ.get("SERP_API_KEY", "")
APIFY_TOKEN     = os.environ.get("APIFY_TOKEN", "")
SHEETS_CREDS    = os.environ.get("GOOGLE_SHEETS_CREDS", "")
SPREADSHEET_ID  = os.environ.get("SPREADSHEET_ID", "")
PROXY           = os.environ.get("PROXY_URL", "")

# ============================================================
# НАЛАШТУВАННЯ ПОШУКУ
# ============================================================
DUBAI_LOCATIONS = [
    {"name": "Dubai Marina",  "lat": 25.0810, "lng": 55.1403},
    {"name": "Business Bay",  "lat": 25.1867, "lng": 55.2651},
    {"name": "JLT",           "lat": 25.0686, "lng": 55.1440},
    {"name": "Downtown Dubai","lat": 25.1972, "lng": 55.2744},
    {"name": "DIFC",          "lat": 25.2132, "lng": 55.2810},
    {"name": "Jumeirah",      "lat": 25.2048, "lng": 55.2432},
]

BUSINESS_TYPES = [
    "gym fitness Dubai",
    "crossfit yoga studio Dubai",
    "specialty coffee cafe Dubai",
    "wellness spa Dubai",
    "healthy restaurant Dubai",
]

LINKEDIN_QUERIES = [
    'site:linkedin.com/in "gym owner" "Dubai"',
    'site:linkedin.com/in "fitness manager" "Dubai"',
    'site:linkedin.com/in "F&B manager" "Dubai"',
    'site:linkedin.com/in "cafe owner" "Dubai"',
    'site:linkedin.com/in "wellness manager" "Dubai"',
    'site:linkedin.com/in "hotel F&B" "Dubai"',
    'site:linkedin.com/in "procurement manager" "Dubai" "food"',
    'site:linkedin.com/company "gym" "Dubai"',
    'site:linkedin.com/company "wellness" "Dubai"',
    'site:linkedin.com/company "specialty coffee" "Dubai"',
]

# ============================================================
# HELPERS
# ============================================================
def delay(a=2.0, b=4.0):
    time.sleep(random.uniform(a, b))

def get_proxies():
    return {"http": PROXY, "https": PROXY} if PROXY else None

# ============================================================
# GOOGLE SHEETS
# ============================================================
def get_sheets_client():
    creds_json = json.loads(SHEETS_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def ensure_sheet(spreadsheet, title, hdrs):
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=2000, cols=len(hdrs))
        ws.append_row(hdrs, value_input_option="RAW")
        ws.freeze(rows=1)
    return ws

def append_leads(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"  ✅ Додано {len(rows)} рядків → '{ws.title}'")
    else:
        print(f"  ℹ️  Немає нових рядків для '{ws.title}'")

# ============================================================
# КРОК 1 — Google Maps: знайти бізнеси в Дубаї
# ============================================================
def search_google_maps(query, lat, lng, radius=2000):
    if not GOOGLE_MAPS_KEY:
        return []
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "location": f"{lat},{lng}",
              "radius": radius, "key": GOOGLE_MAPS_KEY}
    results = []
    while True:
        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            if data.get("status") not in ("OK", "ZERO_RESULTS"):
                break
            results.extend(data.get("results", []))
            token = data.get("next_page_token")
            if not token:
                break
            time.sleep(2)
            params = {"pagetoken": token, "key": GOOGLE_MAPS_KEY}
        except Exception as e:
            print(f"    ⚠️ Maps: {e}")
            break
    return results

def get_place_details(place_id):
    if not GOOGLE_MAPS_KEY:
        return {}
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {"place_id": place_id,
              "fields": "name,formatted_address,formatted_phone_number,website,rating",
              "key": GOOGLE_MAPS_KEY}
    try:
        resp = requests.get(url, params=params, timeout=10)
        return resp.json().get("result", {})
    except Exception:
        return {}

def collect_businesses():
    """Збирає бізнеси з Google Maps по всіх районах."""
    print("\n🗺️  GOOGLE MAPS — збір бізнесів...\n")
    seen, businesses = set(), []

    for loc in DUBAI_LOCATIONS:
        for query in BUSINESS_TYPES:
            print(f"  📍 '{query}' у {loc['name']}...")
            places = search_google_maps(query, loc["lat"], loc["lng"])
            new = 0
            for place in places:
                pid = place.get("place_id")
                if pid in seen:
                    continue
                seen.add(pid)
                details = get_place_details(pid)
                time.sleep(0.2)
                businesses.append({
                    "place_id": pid,
                    "name":     details.get("name", place.get("name", "")),
                    "address":  details.get("formatted_address", ""),
                    "phone":    details.get("formatted_phone_number", ""),
                    "website":  details.get("website", ""),
                    "rating":   details.get("rating", ""),
                    "area":     loc["name"],
                    "category": query,
                })
                new += 1
            print(f"    → {new} нових бізнесів")
            delay(0.5, 1.5)

    print(f"\n  📊 Всього бізнесів: {len(businesses)}")
    return businesses

# ============================================================
# КРОК 2 — SerpAPI: знайти Instagram акаунт бізнесу
# ============================================================
def find_instagram_via_serp(business_name):
    """Шукає Instagram сторінку бізнесу через Google."""
    if not SERP_API_KEY:
        return ""
    query = f'site:instagram.com "{business_name}" Dubai'
    try:
        resp = requests.get("https://serpapi.com/search", params={
            "api_key": SERP_API_KEY,
            "engine": "google",
            "q": query,
            "num": 3,
            "hl": "en",
        }, timeout=15)
        data = resp.json()
        for item in data.get("organic_results", []):
            link = item.get("link", "")
            # Шукаємо прямий профіль instagram.com/username
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)/?$', link)
            if match:
                username = match.group(1)
                # Фільтруємо службові сторінки
                if username not in ("p", "reel", "explore", "stories"):
                    return username
        delay(1, 2)
    except Exception as e:
        print(f"    ⚠️ SerpAPI Instagram search: {e}")
    return ""

# ============================================================
# КРОК 3 — Apify: витягнути деталі Instagram профілю
# ============================================================
def scrape_instagram_profiles_apify(usernames):
    """Запускає Apify Instagram Profile Scraper для списку username."""
    if not APIFY_TOKEN or not usernames:
        return {}

    print(f"\n  🤖 Apify: збір {len(usernames)} Instagram профілів...")

    # Запустити Actor
    run_url = "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs"
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}",
               "Content-Type": "application/json"}
    payload = {
        "usernames": usernames,
        "resultsType": "details",
    }

    try:
        resp = requests.post(run_url, json=payload, headers=headers, timeout=30)
        run_data = resp.json().get("data", {})
        run_id = run_data.get("id")
        dataset_id = run_data.get("defaultDatasetId")

        if not run_id:
            print("    ⚠️ Apify: не вдалось запустити Actor")
            return {}

        # Чекати завершення (максимум 3 хвилини)
        print("    ⏳ Чекаємо Apify...")
        for _ in range(36):
            time.sleep(5)
            status_resp = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                headers=headers, timeout=10)
            status = status_resp.json().get("data", {}).get("status", "")
            if status == "SUCCEEDED":
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                print(f"    ⚠️ Apify Actor: {status}")
                return {}

        # Отримати результати
        items_resp = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            headers=headers, timeout=15)
        items = items_resp.json()

        # Перетворити в словник username → дані
        profiles = {}
        for item in items:
            username = item.get("username", "")
            if username:
                profiles[username] = item
        print(f"    ✅ Apify повернув {len(profiles)} профілів")
        return profiles

    except Exception as e:
        print(f"    ⚠️ Apify помилка: {e}")
        return {}

# ============================================================
# КРОК 4 — LinkedIn через SerpAPI
# ============================================================
def search_linkedin(query):
    if not SERP_API_KEY:
        return []
    print(f"  💼 {query[:60]}...")
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
            print(f"    ⚠️ {data['error']}")
            return []
        results = []
        for item in data.get("organic_results", []):
            link = item.get("link", "")
            if "linkedin.com" not in link:
                continue
            title = item.get("title", "")
            parts = title.split(" - ")
            results.append([
                parts[0].strip() if parts else title,
                parts[1].strip() if len(parts) > 1 else "",
                parts[2].strip() if len(parts) > 2 else "",
                "Company" if "/company/" in link else "Person",
                link,
                item.get("snippet", "")[:150],
                query[:60],
                "Новий",
                datetime.today().strftime("%Y-%m-%d"),
            ])
        delay()
        return results
    except Exception as e:
        print(f"    ⚠️ {e}")
        return []

# ============================================================
# ГОЛОВНА ФУНКЦІЯ
# ============================================================
def run():
    print("=" * 55)
    print("  SKITCHEN Lead Scraper v3")
    print("  Maps → Instagram → LinkedIn → Sheets")
    print(f"  {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55)

    # Підключення до Sheets
    print("\n📊 Підключення до Google Sheets...")
    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print(f"  ✅ '{spreadsheet.title}'")
    except Exception as e:
        print(f"  ❌ {e}")
        return

    ws_maps = ensure_sheet(spreadsheet, "🗺️ Google Maps", [
        "Назва", "Район", "Адреса", "Телефон", "Сайт",
        "Instagram", "IG Followers", "IG Email", "IG Телефон",
        "Рейтинг", "Категорія", "Статус", "Дата",
    ])
    ws_li = ensure_sheet(spreadsheet, "💼 LinkedIn", [
        "Ім'я / Компанія", "Посада", "Компанія", "Тип",
        "LinkedIn URL", "Опис", "Запит", "Статус", "Дата",
    ])
    ws_log = ensure_sheet(spreadsheet, "📋 Лог", [
        "Дата", "Maps бізнесів", "Instagram профілів",
        "LinkedIn лідів", "Всього", "Статус",
    ])

    # ── Крок 1: Google Maps ────────────────────────────────
    businesses = collect_businesses()

    # ── Крок 2: Знайти Instagram username через SerpAPI ────
    print("\n🔍 Шукаємо Instagram акаунти бізнесів...\n")
    for biz in businesses:
        if biz.get("website"):
            # Спробувати знайти username з сайту
            ig_match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', biz["website"])
            if ig_match:
                biz["ig_username"] = ig_match.group(1)
                continue
        # Якщо не знайшли на сайті — шукаємо через SerpAPI
        username = find_instagram_via_serp(biz["name"])
        biz["ig_username"] = username
        if username:
            print(f"  ✅ {biz['name']} → @{username}")
        time.sleep(0.5)

    # ── Крок 3: Apify — деталі Instagram профілів ─────────
    usernames = [b["ig_username"] for b in businesses if b.get("ig_username")]
    ig_profiles = scrape_instagram_profiles_apify(list(set(usernames)))

    # ── Збереження Maps + Instagram даних ─────────────────
    print("\n💾 Збереження в Google Sheets...\n")
    maps_rows = []
    for biz in businesses:
        username = biz.get("ig_username", "")
        profile = ig_profiles.get(username, {})
        bio = profile.get("biography", "")
        email_m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", bio)
        phone_m = re.search(r"(\+971|00971)[\s\-]?[0-9]{8,10}", bio)

        maps_rows.append([
            biz["name"],
            biz["area"],
            biz["address"],
            biz["phone"] or profile.get("businessPhoneNumber", ""),
            biz["website"] or profile.get("externalUrl", ""),
            f"@{username}" if username else "",
            profile.get("followersCount", ""),
            email_m.group() if email_m else profile.get("businessEmail", ""),
            phone_m.group() if phone_m else "",
            biz["rating"],
            biz["category"],
            "Новий",
            datetime.today().strftime("%Y-%m-%d"),
        ])

    append_leads(ws_maps, maps_rows)

    # ── Крок 4: LinkedIn ───────────────────────────────────
    print("\n💼 LINKEDIN (SerpAPI)...\n")
    li_rows = []
    for query in LINKEDIN_QUERIES:
        li_rows.extend(search_linkedin(query))

    # Дедублікація
    seen_urls, unique_li = set(), []
    for row in li_rows:
        if row[4] not in seen_urls:
            seen_urls.add(row[4])
            unique_li.append(row)

    append_leads(ws_li, unique_li)

    # ── Лог ────────────────────────────────────────────────
    total = len(maps_rows) + len(unique_li)
    ws_log.append_row([
        datetime.today().strftime("%Y-%m-%d %H:%M"),
        len(businesses),
        len(ig_profiles),
        len(unique_li),
        total,
        "✅ Успішно",
    ])

    print(f"\n{'=' * 55}")
    print(f"  ✅ ГОТОВО!")
    print(f"  🗺️  Google Maps: {len(businesses)} бізнесів")
    print(f"  📸 Instagram:   {len(ig_profiles)} профілів")
    print(f"  💼 LinkedIn:    {len(unique_li)} лідів")
    print(f"  📊 Всього:      {total} записів")
    print(f"{'=' * 55}")

if __name__ == "__main__":
    run()
