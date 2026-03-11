import pandas as pd
import json
import re
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import ollama
import time
from concurrent.futures import ThreadPoolExecutor, as_completed



BASE_URL = "https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Romance/zgbs/digital-text/6190484011"

# -----------------------------
# STEP 1 — LOAD PAGE HTML
# -----------------------------

def load_page_html(url):

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)  # Keep visible for debugging
        page = browser.new_page()

        page.goto(url)

        # Wait for initial load
        page.wait_for_timeout(3000)

        # Amazon bestseller pages use lazy loading triggered by scrolling near the bottom
        # Scroll to the last visible book to trigger loading of more
        max_attempts = 20
        previous_count = 0

        for attempt in range(max_attempts):
            # Get current count
            current_count = page.locator(".zg-grid-general-faceout").count()
            print(f"Attempt {attempt + 1}: Found {current_count} books")

            if current_count == previous_count and attempt > 0:
                # No new books loaded in last attempt
                break

            previous_count = current_count

            # Scroll the last book into view to trigger lazy loading
            if current_count > 0:
                last_book = page.locator(".zg-grid-general-faceout").nth(current_count - 1)
                last_book.scroll_into_view_if_needed()
                time.sleep(2)  # Wait for potential loading

        # Final wait to ensure all content is loaded
        page.wait_for_timeout(3000)

        html = page.content()

        browser.close()

    return html

# -----------------------------
# STEP 2 — EXTRACT BOOK BLOCKS
# -----------------------------

def extract_books(html):

    soup = BeautifulSoup(html, "lxml")

    books = soup.select(".zg-grid-general-faceout")

    return books


# -----------------------------
# STEP 3 — AI EXTRACTION PROMPT
# -----------------------------

def extract_with_ai(html_block):

    prompt = f"""
You are a data extraction engine.

Extract the following fields from this Amazon bestseller book HTML.

Fields:
- rank: The bestseller rank number (e.g., "1", "2", etc.)
- title: The book title
- author: The author name(s)
- rating: The average rating (e.g., "4.5")
- reviews: The number of reviews (e.g., "1234")
- price: The price (e.g., "$9.99")
- url: The book URL

Return ONLY valid JSON with these exact field names.

Example format:
{{
"rank":"1",
"title":"Book Title",
"author":"Author Name",
"rating":"4.5",
"reviews":"1234",
"price":"$9.99",
"url":"https://www.amazon.com/..."
}}

HTML:
{html_block}
"""

    response = ollama.chat(
        model="llama3",
        messages=[{"role": "user", "content": prompt}]
    )

    text = response["message"]["content"]

    # extract JSON safely
    match = re.search(r"\{.*\}", text, re.S)

    if match:
        try:
            return json.loads(match.group())
        except:
            return None

    return None


# -----------------------------
# STEP 3.5 — EXTRACT BOOK DETAILS FROM INDIVIDUAL PAGES
# -----------------------------

def extract_book_details(book_url):
    """Extract description, publisher, and publication date from individual book page using LLM"""

    # Ensure URL is absolute
    if not book_url.startswith("http"):
        book_url = "https://www.amazon.com" + book_url

    print(f"Navigating to: {book_url}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Change to False to see the page
            page = browser.new_page()

            page.goto(book_url)
            page.wait_for_timeout(3000)  # Wait for page to load

            # Check for "Continue to page" button and click if present
            try:
                continue_button = page.locator("button:has-text('Continue shopping')").first
                if continue_button.is_visible(timeout=5000):
                    continue_button.click()
                    page.wait_for_timeout(3000)  # Wait after clicking
            except:
                pass

            print(f"Page title: {page.title()}")
            
            # Click "Read more" if present
            try:
                read_more = page.query_selector(
                    '#bookDescription_feature_div a[data-action="a-expander-toggle"]'
                )
                
                if read_more:
                    read_more.click()
                    page.wait_for_timeout(1000)

            except:
                pass
            
            # Get the full page HTML
            html = page.content()

            browser.close()

            # Use LLM to extract the details
            return extract_details_with_ai(html)

    except Exception as e:
        print(f"Error extracting details from {book_url}: {e}")
        return {
            "description": "",
            "publisher": "",
            "publication_date": ""
        }


def extract_relevant_sections(html):

    soup = BeautifulSoup(html, "lxml")

    description_text = ""
    details_text = ""

    # ---------- DESCRIPTION ----------
    desc_selectors = [
        "#bookDescription_feature_div",
        "#productDescription",
        "#editorialReviews_feature_div"
    ]

    for sel in desc_selectors:
        div = soup.select_one(sel)
        if div:
            description_text = div.get_text(" ", strip=True)
            break


    # ---------- DETAILS ----------
    detail_selectors = [
        "#detailBullets_feature_div",
        "#detailBulletsWrapper_feature_div",
        "#productDetailsTable"
    ]

    for sel in detail_selectors:
        div = soup.select_one(sel)
        if div:
            details_text = div.get_text(" ", strip=True)
            break


    return description_text, details_text

def extract_details_with_ai(html):

    description_text, details_text = extract_relevant_sections(html)

    prompt = f"""
    You are a data extraction engine.

    Extract the following fields from the provided Amazon Kindle book page content.

    Fields:
    - description: full book description text
    - publisher: publisher name
    - publication_date: publication date

    Rules:
    - Return ONLY JSON
    - Do NOT invent values
    - If a field is missing return an empty string

    JSON schema:
    {{
    "description":"",
    "publisher":"",
    "publication_date":""
    }}

    CONTENT:

    DESCRIPTION:
    {description_text}

    DETAILS:
    {details_text}
        """

    # print("DESCRIPTION TEXT:", description_text[:300])
    # print("DETAILS TEXT:", details_text)

    response = ollama.chat(
        model="llama3",
        messages=[{"role": "user", "content": prompt}]
    )

    text = response["message"]["content"]

    match = re.search(r"\{.*\}", text, re.S)

    if match:
        try:
            data = json.loads(match.group())
            return {
                "description": data.get("description",""),
                "publisher": data.get("publisher",""),
                "publication_date": data.get("publication_date","")
            }
        except:
            pass

    return {
        "description":"",
        "publisher":"",
        "publication_date":""
    }

# -----------------------------
# STEP 4 — SCRAPE BOTH PAGES
# -----------------------------

def scrape_bestsellers():

    urls = [
        BASE_URL,
        BASE_URL + "?pg=2"
    ]

    results = []

    for url in urls:

        print(f"Scraping: {url}")

        html = load_page_html(url)

        books = extract_books(html)
        print("Books found:", len(books))

        for i, book in enumerate(books):

            data = extract_with_ai(str(book))

            if data:
                # Add rank based on position
                data["rank"] = str(len(results) + 1)

                results.append(data)

    # Parallel processing for book details
    print(f"Extracting details for {len(results)} books in parallel...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_book = {executor.submit(extract_book_details, book["url"]): book for book in results if book.get("url")}

        for future in as_completed(future_to_book):
            book = future_to_book[future]
            try:
                details = future.result()
                book.update(details)
                print(f"Completed details for: {book.get('title', 'Unknown')}")
            except Exception as exc:
                print(f"Error extracting details for {book.get('title', 'Unknown')}: {exc}")

    return pd.DataFrame(results)


# -----------------------------
# STEP 5 — CLEAN DATA
# -----------------------------

def clean_data(df):

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    df["reviews"] = (
        df["reviews"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )

    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce")

    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace("$", "", regex=False)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


# -----------------------------
# MAIN PIPELINE
# -----------------------------

def main():

    df = scrape_bestsellers()

    df = clean_data(df)

    df.to_csv("Kindle_Dataset.csv", index=False)

    print("Dataset saved: Kindle_Dataset.csv")
    print(df.head())
    
    # tst = extract_book_details("/Bears-Chosen-Mate-Thornberg-Restaurant-ebook/dp/B0GNR8BCZV/ref=zg_bs_g_6190484011_d_sccl_99/145-5761279-6517218?psc=1")
    # print(tst)


if __name__ == "__main__":
    main()