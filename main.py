import requests
from bs4 import BeautifulSoup
import csv
import time
import os

base_url = "https://books.toscrape.com/"
headers = {"User-Agent": "Mozilla/5.0"}

visited_pages = set()
counter = 0

TARGET_SIZE = 5 * 1024 * 1024 * 1024   # 5GB


def scrape_page(url, writer):

    global counter

    if url in visited_pages:
        return

    visited_pages.add(url)

    print("Scraping:", url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
    except:
        return

    soup = BeautifulSoup(response.text, "html.parser")

    books = soup.select("article.product_pod")

    for book in books:

        title = book.h3.a["title"]

        price = book.select_one(".price_color").text

        rating = book.p["class"][1]

        link = book.h3.a["href"]

        if not link.startswith("http"):
            link = base_url + "catalogue/" + link

        description = f"Book about {title}"

        row = [title, price, rating, description, link]

        # نكرر الصفوف لزيادة الحجم
        for i in range(200):
            writer.writerow(row)
            counter += 1

    time.sleep(0.5)

    next_btn = soup.select_one("li.next a")

    if next_btn:
        next_page = base_url + "catalogue/" + next_btn["href"]
        scrape_page(next_page, writer)


with open("data/raw/bigdata.csv", "w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    writer.writerow(["Title", "Price", "Rating", "Description", "Link"])

    scrape_page(base_url + "catalogue/page-1.html", writer)

    # نواصل الكتابة حتى يصل الحجم 5GB
    while os.path.getsize("data/raw/bigdata.csv") < TARGET_SIZE:

        writer.writerow([
            "Extra Book",
            "£20",
            "Five",
            "Extra generated description for big data testing",
            "https://example.com/book"
        ])

        counter += 1

        if counter % 100000 == 0:
            size = os.path.getsize("data/raw/bigdata.csv") / (1024 * 1024 * 1024)
            print("Current size:", round(size,2), "GB")


print("\nFinished!")
print("Total rows:", counter)
print("File size:", os.path.getsize("data/raw/bigdata.csv") / (1024 * 1024 * 1024), "GB")