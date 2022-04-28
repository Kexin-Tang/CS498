import redis
import pandas as pd

from flask import Flask, render_template, request, session, url_for, redirect
import json
import sys
import os
import datetime
from util import checkAvailable, getCityCode, countReview, noAvailableNeighborhood


app = Flask(__name__)

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)   # host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
r = redis.Redis(connection_pool=pool)

@app.route('/')
def index_page():
	return render_template("index.html")


@app.route('/AirBnB-search', methods=["GET"])
def airBnB_search():
	return render_template("airbnb-search.html")


@app.route('/two-day-availability', methods=["POST"])
def get_two_day_availability():
	start_date = request.form.get('start-date')
	end_date = None
	try:
		end_date = request.form.get('end-date')
	except:
		start_date_object = datetime.date.fromisoformat(start_date)
		end_date = start_date_object + datetime.timedelta(days=1)
	city = request.form.get('city')
	res = checkAvailable(r, getCityCode(city), start_date, end_date.strftime("%Y-%m-%d"))
	data = []
	for item in res:
		data.append(item[1])
	#return redirect(url_for('.airBnB_search', data=data))
	return render_template("airbnb-search.html", data=data)

@app.route('/City-reviews', methods=["POST"])
def City_reviews():
	year = request.form.get('Year')
	month = request.form.get('Month')
	city = request.form.get('city')
	citycode = getCityCode(city)
	res = []
	month = month.zfill(2)
	if year and month and city:
		res.append({"city":city, "reviews":countReview(r, citycode, year, month)})
	if city == None:
		for item in ["LA", "SD", "Salem", "Portland"]:
			res.append({"city":item, "reviews": countReview(r, getCityCode(item), year, month)})
	return render_template("index.html", data=res)

@app.route('/Neighborhoods-search')
def neighborhood_search():
	return render_template("noAvailableNeighborhood.html")

@app.route('/noAvailableNeighborhood', methods=["POST"])
def get_noAvailableNeighborhood():
	year = request.form.get('Year')
	month = request.form.get('Month')
	city = request.form.get('city')
	month = month.zfill(2)
	data = []
	citycode = getCityCode(city)
	res = noAvailableNeighborhood(r, citycode, year, month)
	data.append({"city": city, "neighbourhood": res})
	return render_template("noAvailableNeighborhood.html", data=data)


if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0",  port=80)