""" Responsible for encode/decode object into a writable/readable json """
import json
from datetime import date

from src.record import DailyRecord, Movie

""" Movie Ranking Encoder/Decoder. """


class MovieRankingEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Movie):
            # there should only be 1 entry in obj.revenues. So just retrieve it
            nth_day = [k for k in obj.revenues][0]
            return {
                "id": obj.id,
                "title": obj.title,
                "nth_day": nth_day,
                "ranking": obj.revenues[nth_day].ranking,
                "revenue": obj.revenues[nth_day].revenue,
            }
        return super().default(obj)


def encode_ranking(movies: list[Movie]) -> bytes:
    return json.dumps(movies, cls=MovieRankingEncoder).encode("utf-8")


class MovieRankingDecoder(json.JSONDecoder):
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=MovieRankingDecoder.decode_ranking)

    @staticmethod
    def decode_ranking(d: dict) -> Movie:
        revenues = {d["nth_day"]: DailyRecord(d["ranking"], d["revenue"])}
        return Movie(id=d["id"], title=d["title"], revenues=revenues)


def decode_ranking(readable: bytes) -> list[Movie]:
    return json.loads(readable, cls=MovieRankingDecoder)


""" Movie Encoder/Decoder. """


class MovieEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Movie):
            return {
                "id": obj.id,
                "title": obj.title,
                "release_date": obj.release_date.isoformat(),
                "revenues": obj.revenues,
                "num_of_theaters": obj.num_of_theaters,
                "distributor": obj.distributor,
            }
        elif isinstance(obj, DailyRecord):
            return {"ranking": obj.ranking, "revenue": obj.revenue}
        return super().default(obj)


def encode_movie(movie: Movie) -> bytes:
    return json.dumps(movie, cls=MovieEncoder).encode("utf-8")


class MovieDecoder(json.JSONDecoder):
    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=MovieDecoder.decode_movie)

    @staticmethod
    def decode_movie(d: dict) -> Movie:
        if "id" in d:
            return Movie(
                id=d["id"],
                title=d["title"],
                release_date=date.fromisoformat(d["release_date"]),
                revenues=MovieDecoder.decode_movie(d["revenues"]),
                num_of_theaters=d["num_of_theaters"],
                distributor=d["distributor"],
            )
        elif "ranking" in d and "revenue" in d:
            return DailyRecord(d["ranking"], d["revenue"])
        return d


def decode_movie(readable: bytes) -> Movie:
    return json.loads(readable, cls=MovieDecoder)
