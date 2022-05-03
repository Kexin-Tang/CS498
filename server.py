import redis
import pandas as pd

from flask import Flask, render_template, request, session, url_for, redirect
import json
import sys
import os
import datetime
from util import checkAvailable, getCityCode, countReview, noAvailableNeighborhood


app = Flask(__name__)

# # connect to the first redis server
# pool1 = redis.ConnectionPool(host='localhost', port=6379, password="12345", decode_responses=True)
# r1 = redis.Redis(connection_pool=pool1)
#
# # connect to the second redis server
# pool2 = redis.ConnectionPool(host='172.16.209.78', password="12345", port=6379, decode_responses=True)
# r2 = redis.Redis(connection_pool=pool2)

# # for the distributed storage, decide which redis server the data is stored in
# def get_redis(code):
# 	if code == 1 or code == 2:
# 		return r2
# 	else:
# 		return r1

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)

# load the index page
@app.route('/')
def index_page():
	return render_template("index.html")

# load the page for starting Airbnb Search (Query 1)
@app.route('/AirBnB-search', methods=["GET"])
def airBnB_search():
	return render_template("airbnb-search.html")

# handle Query 1
@app.route('/two-day-availability', methods=["POST"])
def get_two_day_availability():
	"""
		user input from form: city, start-data, end-date(optional)
		output: show all available listings with descending order in rating.
	"""
	start_date = request.form.get('start-date')
	end_date = request.form.get('end-date')
	try:
		start_date_object = datetime.date.fromisoformat(start_date)
	except:
		return render_template("airbnb-search.html", error="Start Date is required!")

	start_date_object = datetime.date.fromisoformat(start_date)
	if len(end_date) == 0:
		end = start_date_object + datetime.timedelta(days=1)
		end_date = end.strftime("%Y-%m-%d")
	city = request.form.get('city')
	city_code = getCityCode(city)
	# res = checkAvailable(get_redis(city_code), getCityCode(city), start_date, end_date)
	res = checkAvailable(r, getCityCode(city), start_date, end_date)
	data = []
	for item in res:
		data.append(item[1])
	#return redirect(url_for('.airBnB_search', data=data))
	return render_template("airbnb-search.html", data=data)

# handle Query 5
@app.route('/City-reviews', methods=["POST"])
def City_reviews():
	"""
		user input from form: city, Year, Month
		output: number of reviews of the city in the selected month
	"""
	year = request.form.get('Year')
	month = request.form.get('Month')
	city = request.form.get('city')
	citycode = getCityCode(city)
	res = []
	month = month.zfill(2)
	if year and month and city:
		# res.append({"city":city, "reviews":countReview(get_redis(citycode), citycode, year, month)})
		res.append({"city": city, "reviews": countReview(r, citycode, year, month)})
	if city == None:
		for item in ["LA", "SD", "Salem", "Portland"]:
			# res.append({"city":item, "reviews": countReview(get_redis(citycode), getCityCode(item), year, month)})
			res.append({"city": item, "reviews": countReview(r, getCityCode(item), year, month)})
	return render_template("index.html", data=res)

# load the page for no-listings neighborhoods search (Query 2)
@app.route('/Neighborhoods-search')
def neighborhood_search():
	return render_template("noAvailableNeighborhood.html")

# handle Query 2
@app.route('/noAvailableNeighborhood', methods=["POST"])
def get_noAvailableNeighborhood():
	"""
		user input from form: city, Year, Month
		output: the list of neighborhoods with no available listings in the given month
	"""
	year = request.form.get('Year')
	month = request.form.get('Month')
	city = request.form.get('city')
	month = month.zfill(2)
	data = []
	citycode = getCityCode(city)
	# res = noAvailableNeighborhood(get_redis(citycode), citycode, year, month)
	res = noAvailableNeighborhood(r, citycode, year, month)
	data.append({"city": city, "neighbourhood": res})
	return render_template("noAvailableNeighborhood.html", data=data)


if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0",  port=80)