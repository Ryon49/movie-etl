import re
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

from .record import Movie, DailyRecord

BASE_URL = "https://www.boxofficemojo.com/"


def string_to_number(value: str):
    """Convert the dollar value into an integer
    E.g. "$3,875,483" => 3875483
    """
    number = value.replace("$", "").replace(",", "")

    # Just in case
    try:
        # convert the string to an integer
        number = int(number)
        return number
    except ValueError:
        raise ValueError(f"value {value} failed to convert to an integer")


""" An example record, 2023-8-17, Barbie.
<tr>
   <td class="a-text-right mojo-header-column mojo-truncate mojo-field-type-rank mojo-sort-column" style="width: 34px; height: 31px; min-width: 34px; min-height: 31px;">1</td>
   <td class="a-text-right mojo-field-type-positive_integer" style="width: 34px; height: 31px; min-width: 34px; min-height: 31px;">1</td>
   <td class="a-text-left mojo-field-type-release mojo-cell-wide" style="width: 243px; height: 31px; min-width: 243px; min-height: 31px;"><a class="a-link-normal" href="/release/rl1077904129/?ref_=bo_da_table_1">Barbie</a></td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 83px; height: 31px; min-width: 83px; min-height: 31px;">$3,875,483</td>
   <td class="a-text-right mojo-number-negative mojo-number-delta mojo-field-type-percent_delta mojo-estimatable" style="width: 59px; height: 31px; min-width: 59px; min-height: 31px;">-14.2%</td>
   <td class="a-text-right mojo-number-negative mojo-number-delta mojo-field-type-percent_delta mojo-estimatable" style="width: 61px; height: 31px; min-width: 61px; min-height: 31px;">-45.8%</td>
   <td class="a-text-right mojo-field-type-positive_integer mojo-estimatable" style="width: 73px; height: 31px; min-width: 73px; min-height: 31px;">4,178</td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 55px; height: 31px; min-width: 55px; min-height: 31px;">$927</td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 99px; height: 31px; min-width: 99px; min-height: 31px;">$545,782,865</td>
   <td class="a-text-right mojo-field-type-positive_integer" style="width: 46px; height: 31px; min-width: 46px; min-height: 31px;">28</td>
   <td class="a-text-left mojo-field-type-release_studios" style="width: 188px; height: 31px; min-width: 188px; min-height: 31px;">
      <a class="a-link-normal" target="_blank" rel="noopener" href="https://pro.imdb.com/company/co0002663/boxoffice/?view=releases&amp;ref_=mojo_da_table_1&amp;rf=mojo_da_table_1">
         Warner Bros.
         <svg class="mojo-new-window-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
            <path d="M24,15.57251l3,3V23.5A3.50424,3.50424,0,0,1,23.5,27H8.5A3.50424,3.50424,0,0,1,5,23.5V8.5A3.50424,3.50424,0,0,1,8.5,5h4.92755l3,3H8.5a.50641.50641,0,0,0-.5.5v15a.50641.50641,0,0,0,.5.5h15a.50641.50641,0,0,0,.5-.5ZM19.81952,8.56372,12.8844,17.75a.49989.49989,0,0,0,.04547.65479l.66534.66528a.49983.49983,0,0,0,.65479.04553l9.18628-6.93518,2.12579,2.12585a.5.5,0,0,0,.84741-.27526l1.48273-9.35108a.50006.50006,0,0,0-.57214-.57214L17.969,5.59058a.5.5,0,0,0-.27526.84741Z"></path>
         </svg>
      </a>
   </td>
   <td class="a-text-right mojo-field-type-boolean hidden" style="width: 0px; height: 0px; min-width: 0px; min-height: 0px;">false</td>
   <td class="a-text-right mojo-field-type-boolean hidden" style="width: 0px; height: 0px; min-width: 0px; min-height: 0px;">false</td>
</tr>

Field of interest: 
    - mojo-field-type-rank: 
        - today's ranking
    - mojo-field-type-release:
        Movie title
    - mojo-field-type-money:
        0. today's revenue
        1. Ignored, average revenue per theather
        2. Ignored, total revenue
    - mojo-field-type-positive_integer: (save to convert)
        0. yesterday's ranking
        1. Estimated # of theaters showing
        2. # of days in theaters
    - mojo-field-type-release_studios
        publishing studio 
    
"""


def crawl_daily_ranking(d: date) -> list[Movie]:
    """Crawl daily ranking
    Args:
        d: a date object.
    Returns:
        A list of parsed movie record.

    """
    id_pattern = r"/release/(\w+)/\?ref_"

    target_url = f"{BASE_URL}/date/{d.isoformat()}"

    r = requests.get(target_url)
    soup = BeautifulSoup(r.text, features="html.parser")

    # navigate to the table containing the ranking.
    table_html = soup.find(id="table")
    # extracts the <tr> rows that contains the information, ignore the header row
    records = table_html.find_all("tr")[1:]

    movies: list[Movie] = []
    for record in records:
        rank: str = string_to_number(
            record.find(class_="mojo-field-type-rank").text.strip()
        )
        title: str = record.find(class_="mojo-field-type-release").a.text.strip()
        href: str = record.find(class_="mojo-field-type-release").a["href"]
        href: str = re.findall(id_pattern, href)[0]

        # handle all money
        daily_gross: int = string_to_number(
            record.find(class_="mojo-field-type-money").text.strip()
        )

        # handle the numbers
        numbers = record.find_all(class_="mojo-field-type-positive_integer")
        num_of_theaters: int = string_to_number(numbers[1].text) if numbers[1].text.isnumeric() else 0
        num_of_days_in_theater: int = string_to_number(numbers[2].text)

        # because num_of_days_in_theater starts with 1, substract 1
        release_date = d - timedelta(days=num_of_days_in_theater - 1)

        # special case: somtimes a row does not have a "studio" column, simply marked as "None"
        distributor: str = record.find(class_="mojo-field-type-release_studios").a
        distributor = distributor.text.strip() if distributor is not None else "None"

        # print(f"rank = {rank}, title = {title}, daily_gross = {daily_gross}, href = {href} \t \
        #         num_of_theaters = {num_of_theaters}, num_of_days_in_theaters = {num_of_days_in_theaters},\t \
        #         studio = {studio}")

        m = Movie(
            id=href,
            title=title,
            release_date=release_date,
            revenues={num_of_days_in_theater: DailyRecord(rank, daily_gross)},
            num_of_theaters=num_of_theaters,
            distributor=distributor,
        )

        movies.append(m)
    return movies


""" An example summary of Barbie's summary
<div class="a-section a-spacing-none mojo-summary-values mojo-hidden-from-mobile">
   ...
   <div class="a-section a-spacing-none"><span>Release Date</span><span><a class="a-link-normal" href="/date/2023-07-21/?ref_=bo_rl_rl">Jul 21, 2023</a></span></div>
   <div class="a-section a-spacing-none"><span>MPAA</span><span>PG-13</span></div>
   <div class="a-section a-spacing-none"><span>Running Time</span><span>1 hr 54 min</span></div>
   <div class="a-section a-spacing-none"><span>Genres</span><span>Adventure
      Comedy
      Fantasy</span>
   </div>
   ...
</div>
"""
""" An example record of Barbie on 2023-7-21
<tr>
   <td class="a-text-left mojo-header-column mojo-truncate mojo-field-type-date_interval mojo-sort-column" style="width: 85px; height: 34px; min-width: 85px; min-height: 34px;"><a class="a-link-normal" href="/date/2023-07-21/?ref_=bo_rl_table_1">Jul 21</a></td>
   <td class="a-text-left mojo-field-type-date_interval" style="width: 116px; height: 34px; min-width: 116px; min-height: 34px;"><a class="a-link-normal" href="/date/2023-07-21/?ref_=bo_rl_table_1">Friday</a></td>
   <td class="a-text-right mojo-field-type-rank" style="width: 77px; height: 34px; min-width: 77px; min-height: 34px;">1</td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 126px; height: 34px; min-width: 126px; min-height: 34px;">$70,503,178</td>
   <td class="a-text-right mojo-field-type-percent_delta mojo-estimatable" style="width: 90px; height: 34px; min-width: 90px; min-height: 34px;">-</td>
   <td class="a-text-right mojo-field-type-percent_delta mojo-estimatable" style="width: 86px; height: 34px; min-width: 86px; min-height: 34px;">-</td>
   <td class="a-text-right mojo-field-type-positive_integer mojo-estimatable" style="width: 105px; height: 34px; min-width: 105px; min-height: 34px;">4,243</td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 93px; height: 34px; min-width: 93px; min-height: 34px;">$16,616</td>
   <td class="a-text-right mojo-field-type-money mojo-estimatable" style="width: 135px; height: 34px; min-width: 135px; min-height: 34px;">$70,503,178</td>
   <td class="a-text-right mojo-field-type-positive_integer" style="width: 61px; height: 34px; min-width: 61px; min-height: 34px;">1</td>
   <td class="a-text-right mojo-field-type-boolean hidden" style="width: 0px; height: 0px; min-width: 0px; min-height: 0px;">false</td>
</tr>
Field of interest: 
    - mojo-field-type-date_interval: 
        1. date
        2. ignored
    - mojo-field-type-rank:
        ranking
    - mojo-field-type-money
        0. today's revenue
        1. Ignored, average revenue per theather
        2. Ignored, total revenue
    - mojo-field-type-positive_integer:
        1. Ignored, number of theaters
        2. # of days in theaters
"""

""" Regarding to parsing "Release Date", there are couple variant datestrings.
1. id=rl1592820481
    this one has **Release Date\n        \n            (Wide)** instead of "Release Date"
2. id=rl1930593025
    Comparing "Apr 15, 2023\n..." to "Apr 5, 2023\n...", capturing first 12 character may include a new line character
"""


def crawl_movie_detail(id: str) -> Movie:
    target_url = f"{BASE_URL}/release/{id}"

    r = requests.get(target_url)
    soup = BeautifulSoup(r.text, features="html.parser")

    # extract the title
    title = soup.find("h1", class_="a-size-extra-large").text.strip()

    # extract the summary
    # interested in "Distributor", "Release Date", "Widest Release/(# of theaters)"
    summaries = dict()

    for div in soup.find(class_="mojo-summary-values"):
        spans = div.find_all("span")
        key = spans[0].text.strip()
        value = spans[1].text.strip()
        summaries[key] = value

    distributor = summaries["Distributor"].replace("See full company information", "")
    num_of_theaters = string_to_number(
        re.findall(r"\d+", summaries["Widest Release"].replace(",", ""))[0]
    )

    # handle special conditions for "release_date"
    release_date = None
    for key in summaries:
        if key.startswith("Release Date"):
            release_date = datetime.strptime(
                summaries[key][:12].strip(), "%b %d, %Y"
            ).date()

    # navigate to the table containing the ranking.
    table_html = soup.find(id="table")
    # extracts the <tr> rows that contains the information, ignore the header row
    records = table_html.find_all("tr")[1:]

    revenues = dict()
    for record in records:
        rank: str = record.find(class_="mojo-field-type-rank").text.strip()

        daily_gross: int = string_to_number(
            record.find(class_="mojo-field-type-money").text.strip()
        )

        # handle the numbers
        numbers = record.find_all(class_="mojo-field-type-positive_integer")
        # num_of_theaters: int = string_to_number(numbers[0].text)
        num_of_days_in_theater: int = string_to_number(numbers[1].text)

        # print(f"rank = {rank}, daily_gross = {daily_gross}, \t\
        #       num_of_days_in_theater = {num_of_days_in_theater}")
        revenues[num_of_days_in_theater] = daily_gross

    return Movie(
        id=id,
        title=title,
        release_date=release_date,
        revenues=revenues,
        num_of_theaters=num_of_theaters,
        distributor=distributor,
    )
