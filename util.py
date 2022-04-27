import redis
import re
import datetime

def getListingWithPattern(r: redis.Redis, pattern: str) -> list:
    """
        get listing according to given pattern
    """
    res = []
    for k in r.scan_iter(pattern):
        res.append((k, r.hgetall(k)))
    return res

def getDates(startDate: str, endDate: str) -> list:
    """
        return the list of dates between startDate and endDate

        eg: startDate="2022-03-10", endDate="2022-03-12"
            return ["2022-03-10", "2022-03-11", "2022-03-10"]
    """
    res = []
    start = datetime.datetime.strptime(startDate, "%Y-%m-%d")
    end = datetime.datetime.strptime(endDate, "%Y-%m-%d")
    res.append(start.strftime("%Y-%m-%d"))
    while start < end:
        start += datetime.timedelta(days=1)
        res.append(start.strftime("%Y-%m-%d"))
    return res

def checkAvailabilityInCertainDate(r: redis.Redis, date: str, offset=0) -> str:
    """
        listingID as offset in bitmap
        Check if `listingID` in `date` is available
    """
    return str(r.getbit(date, offset))


def checkAvailable(r: redis.Redis, cityCode: int, startDate: str, endDate: str) -> list:
    """
        Binary-operation of two bitmaps and check the availability
        (offset is 1 -> available)
    """
    dates = getDates(startDate, endDate)
    bitmap_res = r.bitop("AND", "checkAvailable", *dates)   # all days are available
    pattern = "listingID:" + str(cityCode) + "*"
    res = []
    for k in r.scan_iter(pattern):
        offset = int(re.findall(r"\d+\.?\d*", k)[0])
        if r.getbit("checkAvailable", offset):
            res.append((k, r.hgetall(k)))
    return res


def countReview(r: redis.Redis, cityCode: int, year: str, month: str) -> int:
    """
        for certain city, count the review amount in `year-month`
    """
    cnt: int = 0
    pattern = "reviews:" + str(year) + '-' + str(month) + '-*'
    for k in r.scan_iter(pattern):
        v = r.get(k)
        if int(v[0]) == cityCode:
            cnt += 1
    return cnt


def noAvailableNeighborhood(r: redis.Redis, cityCode: int, year: str, month: str) -> set:
    """
        find the no listing neighborhoods in a given month

        NOTICE: month's format, for example, March (3 -> 03)
    """
    # at least one day is available
    pattern = str(year) + "-" + str(month) + "-*"
    vec = []
    for k in r.scan_iter(pattern):
        vec.append(k)
    bitmap_res = r.bitop("OR", "noAvailableNeighborhood", *vec)

    # find available neighbor, store into the availableSet
    findOffset = "listingID:" + str(cityCode) + "*"
    availableNeighbor = set()
    for k in r.scan_iter(findOffset):
        offset = int(re.findall(r"\d+\.?\d*", k)[0])
        if r.getbit("noAvailableNeighborhood", offset):
            availableNeighbor.add(r.hget(k, "neighbourhood_cleansed"))

    # find all neighbor
    allNeighbor = r.smembers(cityCode)

    # result is the diff between allNeighbor w availableNeighbor
    return allNeighbor - availableNeighbor


if __name__ == "__main__":
    from main import r, cityCode

    # print(getListingWithPattern(r, "listingID:4*"))
    # print(checkAvailabilityInCertainDate(r, "2022-03-10", 442100848))
    print(checkAvailable(r, 4, "2022-03-10", "2022-03-31"))
    # print(countReview(r, 4, "2019", "12"))
    # citys = [3, 4]
    # for city in citys:
    #     print(noAvailableNeighborhood(r, city, "2022", "03"))
