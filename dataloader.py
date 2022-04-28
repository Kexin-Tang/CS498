import redis
import pandas as pd
import datetime
import os
import sys

pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    decode_responses=True
)
r = redis.Redis(connection_pool=pool)

cityCode = {
    "LA": 1,
    "SD": 2,
    "Portland": 3,
    "Salem": 4
}


def modifyListingID(file_path: str, code: int) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df["listing_id"] = str(code) + df[str(df.columns.tolist()[0])].astype(str)
    return df


def insertNeighborhood(file_path: str, code: int):
    df = pd.read_csv(file_path)
    for neighborhood in df["neighbourhood"]:
        r.sadd(code, neighborhood)


def insertReviews(df: pd.DataFrame):
    df["date-reviewer"] = df["date"] + ":" + df["reviewer_id"].astype(str)
    for _, row in df.iterrows():
        key = "reviews:" + row["date-reviewer"]
        value = row["listing_id"]
        r.set(key, value)


def insertListing(df: pd.DataFrame, useCols: list):
    df['review_scores_rating'] = df['review_scores_rating'].fillna(0)
    df['review_scores_accuracy'] = df['review_scores_accuracy'].fillna(0)
    df['review_scores_cleanliness'] = df['review_scores_cleanliness'].fillna(0)
    df['review_scores_checkin'] = df['review_scores_checkin'].fillna(0)
    df['review_scores_communication'] = df['review_scores_communication'].fillna(0)
    for _, row in df.iterrows():
        key = 'listingID:' + row["listing_id"]
        for col in useCols:
            r.hset(key, col, row[col])


def insertBitmap(df: pd.DataFrame, useCols: list):
    for _, row in df.iterrows():
        date = row['date']
        offset = int(row['listing_id'])
        if row['available'] == 't':
            r.setbit(date, offset, 1)
        else:
            r.setbit(date, offset, 0)

def areaCode(cityCode: dict):
    for key, value in cityCode.items():
        r.hset("Area Code", key, value)


def init():
    neighborPaths = ["./dataset/Portland/neighbourhoods.csv"]
    reviewPaths = ["./dataset/Portland/reviews.csv"]
    calendarPaths = ["./dataset/Portland/calendar.csv"]
    listingPaths = ["./dataset/Portland/listings.csv"]
    areaCode(cityCode)

    for item in neighborPaths:
        city = item.split("/")[-2]
        code = cityCode[city]
        insertNeighborhood(item, code)

    print("neighbor finished")

    for item in reviewPaths:
        city = item.split("/")[-2]
        code = cityCode[city]
        df = modifyListingID(item, code)
        insertReviews(df)

    print("review finished")

    for item in calendarPaths:
        city = item.split("/")[-2]
        code = cityCode[city]
        df = modifyListingID(item, code)
        insertBitmap(df, ['listing_id', 'date', 'available'])

    print("calendar finished")

    for item in listingPaths:
        city = item.split("/")[-2]
        code = cityCode[city]
        df = modifyListingID(item, code)
        insertListing(df, ['listing_id', 'name', 'room_type', 'minimum_nights', 'maximum_nights', "review_scores_rating", "neighbourhood", "accommodates", "property_type", "price", "neighbourhood_cleansed"])


if __name__ == "__main__":
    init()
    print(str(r.getbit("2022-03-10", 442100848)))
