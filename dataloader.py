import redis
import pandas as pd
import datetime
import os
import sys
from main import cityCode, r


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
    neighborPaths = ["./dataset/Portland/neighbourhoods.csv", "./dataset/Salem/neighbourhoods.csv"]
    reviewPaths = ["./dataset/Portland/reviews.csv", "./dataset/Salem/reviews.csv"]
    calendarPaths = ["./dataset/Portland/calendar.csv", "./dataset/Salem/calendar.csv"]
    listingPaths = ["./dataset/Portland/listings.csv", "./dataset/Salem/listings.csv"]
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
        insertListing(df, ['name', 'room_type', 'minimum_nights', 'maximum_nights', "review_scores_rating", "neighbourhood_cleansed"])

    print("listing finished")

if __name__ == "__main__":
    init()
    # print(str(r.hgetall("listingID:46030321")))
