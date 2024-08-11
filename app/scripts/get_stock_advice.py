import os
import re
import time
import asyncio
import aiohttp
import requests
import warnings
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup, Comment
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio

recent_stock_advice_url = (
    "https://www.moneycontrol.com/news/business/stocks/page-{page_number}/"
)
stock_advice_url = "https://m.moneycontrol.com/more_market.php?sec=stk_adv&ajax=1&start={start}&limit={limit}"
stock_history_url = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={company_code}&resolution=1D&from={From}&to={to}&countback={countback}&currencyCode=INR"

stock_advice_df = pd.DataFrame(
    columns=[
        "Article Date",
        "Advisor Name",
        "Stock Name",
        "Company Code",
        "Call",
        "Price on Article Day",
        "Target Price",
        "Today's Price",
        "Time to reach target",
        "Arbitrage %",
        "Today P/L %",
        "Article URL",
    ]
)

# Suppress the FutureWarning
warnings.simplefilter(action="ignore", category=FutureWarning)


async def async_get(*args, **kwargs):
    async with aiohttp.ClientSession() as session:
        async with session.get(*args, **kwargs) as response:
            return await response.text()


async def async_get_json(*args, **kwargs):
    async with aiohttp.ClientSession() as session:
        async with session.get(*args, **kwargs) as response:
            return await response.json()


async def get_company_code(stock_pricequote_url):
    response = await async_get(stock_pricequote_url)
    soup = BeautifulSoup(response, "html.parser")
    return (
        soup.find("div", {"id": "company_info"})
        .find("span", string="NSE:")
        .find_next_sibling("p")
        .text
    )


async def get_article_time(article_url):
    try:
        response = await async_get(article_url)
        soup = BeautifulSoup(response, "html.parser")
        return soup.find("div", {"class": "article_schedule"}).find("span").text
    except Exception as e:
        return None


def get_target_percent(start_price, target_price):
    return (target_price - start_price) / start_price * 100


async def get_stock_price(company_code, date_string, call, target_price):
    if date_string is None:
        return None, None, None
    if isinstance(date_string, datetime):
        from_date = date_string
    elif "AM" in date_string or "PM" in date_string:
        from_date = datetime.strptime(date_string, "%B %d, %Y %I:%M %p")
    else:
        from_date = datetime.strptime(date_string, "%B %d, %Y")
    to_date = datetime.now()
    from_timestamp = int(time.mktime(from_date.timetuple()))
    to_timestamp = int(time.mktime(to_date.timetuple()))

    actual_num_days = (to_date - from_date).days
    num_days = 1 if actual_num_days == 0 else actual_num_days

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    try:
        response = await async_get_json(
            stock_history_url.format(
                company_code=urllib.parse.quote_plus(company_code),
                From=from_timestamp,
                to=to_timestamp,
                countback=num_days,
            ),
            headers=headers,
        )
        closing_prices = response["c"]
        if actual_num_days == 0:
            closing_prices = [closing_prices[-1]]
        # Consider closing day priceg
        todays_price = closing_prices[-1]
        price_at_article_date = closing_prices[0]

        days_to_reach_target = get_days_to_reach_target(
            call, closing_prices, target_price
        )
        return price_at_article_date, todays_price, days_to_reach_target
    except Exception as e:
        print(e)
        return None, None, None


def get_days_to_reach_target(call, history, target_price):
    call = call.strip().lower()
    buy_variants = ["buy", "accumulate"]
    sell_variants = ["sell", "reduce"]
    if call not in [*buy_variants, *sell_variants]:
        return "Call type not supported."

    if call in buy_variants:
        for i, price in enumerate(history):
            if price >= target_price:
                return i
        return "Not reached yet."
    if call in sell_variants:
        for i, price in enumerate(history):
            if price <= target_price:
                return i
    return "Not reached yet."


async def get_stock_info(news):
    stock_info = news.find("div", {"class": "rb_gd14"})
    article_info = news.find("div", {"class": "MT5"})

    stock_name = stock_info.find("a").text
    buy_sell_hold = stock_info.find("img").get("alt")
    stock_pricequote_url = stock_info.find("a").get("href")
    company_code = await get_company_code(stock_pricequote_url)
    article_url = article_info.find("a").get("href")
    article_title = article_info.find("a").text
    advisor_name = article_title.split(":")[-1].strip()
    target_price = int(
        re.search(r"target of Rs ([\d,]+)", article_title).group(1).replace(",", "")
    )
    article_time = await get_article_time(article_url)
    price_at_article_date, todays_price, days_to_reach_target = await get_stock_price(
        company_code, article_time, buy_sell_hold, target_price
    )
    arbitrage, today_pl = get_percent_change(
        price_at_article_date, target_price, todays_price
    )
    # Append to the dataframe
    stock_advice_df.loc[len(stock_advice_df)] = [
        article_time,
        advisor_name,
        stock_name,
        company_code,
        buy_sell_hold,
        price_at_article_date,
        target_price,
        todays_price,
        days_to_reach_target,
        arbitrage,
        today_pl,
        article_url,
    ]

    return None


async def get_pricequote_url(article_url):
    response = await async_get(article_url)
    soup = BeautifulSoup(response, "html.parser")
    # get this xpath //*[@id="contentdata"]/p[1]/strong/a
    return (
        soup.find("div", {"id": "contentdata"})
        .find("p")
        .find("strong")
        .find("a")
        .get("href")
    )


async def get_company_code_from_name(stock_name):
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

    stock_name = stock_name.rstrip("s")
    if len(stock_name.split(" ")) > 3:
        stock_name = " ".join(stock_name.split(" ")[:3])

    params = {"q": stock_name, "quotes_count": 1, "country": "United States"}

    data = await async_get_json(
        url=url, params=params, headers={"User-Agent": user_agent}
    )

    try:
        company_code = data["quotes"][0]["symbol"]
    except Exception as e:
        return None
    return company_code.split(".")[0]


def get_percent_change(price_on_article, target_price, today_price):
    arbitrage = None
    today_pl = None
    if price_on_article and target_price:
        arbitrage = get_target_percent(price_on_article, target_price)
    if price_on_article and today_price:
        today_pl = get_target_percent(price_on_article, today_price)
    return arbitrage, today_pl


def get_article_time(news):
    article_time = news.find("span")
    if article_time is not None:
        return article_time.text
    for element in news(text=lambda text: isinstance(text, Comment)):
        if "span" in element:
            comment_soup = BeautifulSoup(element, "html.parser")
            article_time = comment_soup.find("span")
            return article_time.text


async def get_article_details(news):
    article_title = news.find("a").get("title")
    article_time = get_article_time(news)
    target_price = int(
        re.search(r"target of Rs ([\d,]+)", article_title).group(1).replace(",", "")
    )
    buy_sell_hold, *stock_name = article_title.split(";")[0].split(" ")
    stock_name = " ".join(stock_name)
    advisor_name = article_title.split(":")[-1].strip()
    article_link = news.find("a").get("href")
    try:
        stock_pricequote_url = await get_pricequote_url(article_link)
        company_code = await get_company_code(stock_pricequote_url)
    except Exception as e:
        company_code = await get_company_code_from_name(stock_name)
    price_at_article_date, todays_price, days_to_reach_target = await get_stock_price(
        company_code,
        article_time.rsplit(" ", 1)[0],
        buy_sell_hold,
        target_price,
    )

    arbitrage, today_pl = get_percent_change(
        price_at_article_date, target_price, todays_price
    )
    # Append to the dataframe
    stock_advice_df.loc[len(stock_advice_df)] = [
        pd.to_datetime(article_time),
        advisor_name,
        stock_name,
        company_code,
        buy_sell_hold,
        price_at_article_date,
        target_price,
        todays_price,
        days_to_reach_target,
        arbitrage,
        today_pl,
        article_link,
    ]


async def get_data(row):
    company_code = row["Company Code"]
    article_time = row["Article Date"]
    buy_sell_hold = row["Call"]
    target_price = row["Target Price"]
    # convert article time to datetime
    if isinstance(article_time, str):
        article_time = datetime.strptime(article_time, "%Y-%m-%d %H:%M:%S")
    price_at_article_date, todays_price, days_to_reach_target = await get_stock_price(
        company_code,
        article_time,
        buy_sell_hold,
        target_price,
    )

    arbitrage, today_pl = get_percent_change(
        price_at_article_date, target_price, todays_price
    )
    return (
        todays_price,
        days_to_reach_target,
        arbitrage,
        today_pl,
    )


async def update_df_stock_price(df):
    tasks = [get_data(row[1]) for row in df.iterrows()]
    results = await asyncio.gather(*tasks)
    for i, result in enumerate(results):
        (
            df.at[i, "Today's Price"],
            df.at[i, "Time to reach target"],
            df.at[i, "Arbitrage %"],
            df.at[i, "Today P/L %"],
        ) = result


def get_recent_articles():
    global stock_advice_df
    total_pages = 30
    current_page = 1
    tasks = []

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    DATA_PATH = os.path.abspath(
        os.path.join(CURRENT_DIR, "..", "..", "data", "stock_advice.csv")
    )
    stock_advice_df = pd.read_csv(DATA_PATH)
    stock_advice_df["Article Date"] = pd.to_datetime(stock_advice_df["Article Date"])
    stop_date = stock_advice_df["Article Date"].iloc[-1]

    is_stopped = False
    while current_page <= total_pages:
        response = requests.get(
            recent_stock_advice_url.format(page_number=current_page)
        )
        soup = BeautifulSoup(response.text, "html.parser")

        for news in soup.find("ul", {"id": "cagetory"}).find_all(
            "li", id=lambda x: x and x.startswith("newslist-")
        ):
            article_title = news.find("a").get("title")
            if "target of" not in article_title:
                continue

            article_time = get_article_time(news)

            _article_time = pd.to_datetime(article_time)
            if _article_time < stop_date:
                is_stopped = True
                break
            tasks.append(get_article_details(news))
        current_page += 1
        if is_stopped:
            break

    eventloop = asyncio.get_event_loop()
    eventloop.run_until_complete(tqdm_asyncio.gather(*tasks))

    # sort by "Article Date" and save
    stock_advice_df.sort_values(by="Article Date", inplace=True)

    stock_advice_df.to_csv(DATA_PATH, index=False)
    print(f"Added {len(tasks)} stock advices to {DATA_PATH}")

    print("Updating the stock prices...")
    eventloop.run_until_complete(update_df_stock_price(stock_advice_df))
    stock_advice_df.to_csv(DATA_PATH, index=False)
    print(f"Stock advice updated in {DATA_PATH}")


def get_old_articles():
    tasks = []
    limit = 100
    print(f"Getting {limit} stock advices from {stock_advice_url}")
    response = requests.get(stock_advice_url.format(start=0, limit=limit))
    soup = BeautifulSoup(response.text, "html.parser")

    print("Interpreting the stock advice...")
    for news in soup.find_all("li"):
        tasks.append(get_stock_info(news))

    eventloop = asyncio.get_event_loop()
    eventloop.run_until_complete(tqdm_asyncio.gather(*tasks))

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    DATA_PATH = os.path.abspath(
        os.path.join(CURRENT_DIR, "..", "..", "data", "old_stock_advice.csv")
    )
    stock_advice_df.to_csv(DATA_PATH, index=False)
    print(f"Stock advice saved to {DATA_PATH}")


if __name__ == "__main__":
    get_recent_articles()
