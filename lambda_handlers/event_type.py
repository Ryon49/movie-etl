from enum import StrEnum

class EventType(StrEnum):
    # Used to reset metadata.json
    RESET = "RESET"
    # Used to debug
    DEBUG = "DEBUG"
    # Used to add new daily ranking
    PREPARE_NEW_DATE = "prepare_new_date"
    # Used to prepare request for crawling ranking
    PREPARE_RANKING = "prepare_ranking"
    # Used to prepare request for crawling ranking
    CRAWL_RANKING = "crawl_ranking"
    # Used to validate crawl ranking result
    VALIDATE_RANKING = "validate_ranking"
    # Used to prepare request for crawling movie details
    PREPARE_MOVIE_DETAIL = "crawl_movie_detail"
    # Used to validate crawl movie details result