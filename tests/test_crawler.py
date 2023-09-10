import pytest
from datetime import date, timedelta, datetime

from src.crawler import crawl_movie_detail, crawl_daily_ranking

@pytest.mark.parametrize("datestring", ["2023-08-17", # basic testing
                                        "2023-08-10", # "num_of_theaters is missing"
                                        ])
def test_crawl_ranking(datestring):
    d = date.fromisoformat(datestring)

    ranking = crawl_daily_ranking(d) 
    
    assert ranking != None
