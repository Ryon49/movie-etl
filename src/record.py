from dataclasses import dataclass, field
from datetime import date


@dataclass
class DailyRecord:
    ranking: int  # ranking for nth date
    revenue: int  # revenue for nth date


@dataclass
class Movie:
    """Class"""

    id: str  # id/href used to retrieve detail website
    title: str  # movie title
    release_date: date = None  # date when movie is in theater

    # using nth day as key, and that day's revenue as value
    revenues: dict[DailyRecord] = field(default_factory=dict)
    num_of_theaters: int = field(default=-1)  # make -1 be no information
    distributor: str = field(default=None)

    def gross_revenue(self):
        # if (max(self.revenues.keys()) != len(self.revenues)):
        #     raise ValueError("Not enough data")
        return sum(r.revenue for r in self.revenues.values())

    def merge_records(self, revenues: dict[DailyRecord]):
        """Usage: add revenues from other day's to this"""
        for nth_day, revenue in revenues.items():
            self.revenues[nth_day] = revenue

    def newest_nth_day(self):
        return max(self.revenues.keys())
