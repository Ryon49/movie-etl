import pytest
import json
from datetime import date

from src.record import DailyRecord, Movie
from src.encoders.json_encoder import encode_movie, decode_movie


def test_encode_movie():
    movie = Movie(
        id="1",
        title="Mock",
        release_date=date.fromisoformat("2023-08-17"),
        revenues={"1": DailyRecord(1, 1000), "2": DailyRecord(3, 700)},
        num_of_theaters=1,
        distributor=None,
    )

    writable = encode_movie(movie).decode("utf-8")
    json_obj = json.loads(writable)

    assert "id" in json_obj and json_obj["id"] == "1"
    assert "title" in json_obj and json_obj["title"] == "Mock"
    assert "release_date" in json_obj and json_obj["release_date"] == "2023-08-17"
    assert "revenues" in json_obj and len(list(json_obj["revenues"].keys())) == 2
    assert "num_of_theaters" in json_obj and json_obj["num_of_theaters"] == 1
    assert "distributor" in json_obj and json_obj["distributor"] == None

def test_decode_move():
    expected = Movie(
        id="1",
        title="Mock",
        release_date=date.fromisoformat("2023-08-17"),
        revenues={"1": DailyRecord(1, 1000), "2": DailyRecord(3, 700)},
        num_of_theaters=1,
        distributor=None,
    )

    writable = encode_movie(expected)
    actual = decode_movie(writable)
    assert actual == expected 