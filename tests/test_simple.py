import pytest
import uuid
from datetime import date, timedelta, datetime

from src.crawler import crawl_movie_detail, crawl_daily_ranking

def test_simple():
    id = "rl1592820481"

    d = date.fromisoformat("2023-08-10")

    # crawl_daily_ranking(d)

    assert not "-".isnumeric()

def test_uuid():

    uuid.uuid4()
    r = uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name="2023-09-07").__str__()
    print(r)