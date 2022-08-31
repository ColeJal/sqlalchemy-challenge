from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()

Base.prepare(engine, reflect=True)

Base.classes.keys()

Measurement = Base.classes.measurement

Station = Base.classes.station


app = Flask(__name__)

@app.route('/')
def index_homepage():
    return (f'All Routes:<br/>'
            f'/api/v1.0/precipitation<br/>'
            f'/api/v1.0/stations<br/>'
            f'/api/v1.0/tobs<br/>'
            f'/api/v1.0/start<br/>'
            f'/api/v1.0/start/end'
            )

@app.route('/api/v1.0/precipitation')
def precipitation():
    session = Session(engine)
    
    recent_time = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    recent_time

    good_date = list(np.ravel(recent_time))[0]

    good_date

    most_recent_data = dt.datetime.strptime(good_date, '%Y-%m-%d')

    year_data = most_recent_data - dt.timedelta(days=365)

    year_data = session.query(Measurement.date,Measurement.prcp).filter(Measurement.date >= year_data).\
    filter(Measurement.date <= most_recent_data).all()
    
    session.close()

    to_return = {}

    for row in year_data:
        to_return[row[0]] = row[1]

    return jsonify(to_return)


@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)

    station_query = session.query(Station.station).count()

    station_query

    active_query = session.query(Measurement.station,func.count(Measurement.station)).\
                group_by(Measurement.station).\
                order_by(func.count(Measurement.station).desc()).all()

    session.close()

    to_return = {}

    for row in active_query:
        to_return[row[0]] = row[1]

    return jsonify(to_return)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    recent_time = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    recent_time

    good_date = list(np.ravel(recent_time))[0]

    good_date

    most_recent_data = dt.datetime.strptime(good_date, '%Y-%m-%d')

    year_data = most_recent_data - dt.timedelta(days=365)

    year_temp_query = session.query(Measurement.station,Measurement.tobs)\
    .filter(Measurement.date >= '2016-08-23')\
    .filter(Measurement.date <= '2017-08-23')\
    .filter(Measurement.station == 'USC00519281').all()

    session.close()

    to_return = {}

    for row in year_temp_query:
        to_return[row[0]] = row[1]

    return jsonify(to_return)


@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end):

    session = Session(engine)


    start_end = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).\
                    filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    session.close()
    temp_obs = {}
    temp_obs['Min_Temp']=start_end[0][0]
    temp_obs['avg_Temp']=start_end[0][1]
    temp_obs['max_Temp']=start_end[0][2]
    return jsonify(temp_obs)


@app.route("/api/v1.0/<start>")
def start_temps(start):
    session = Session(engine)

    start_end = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).\
                    filter(Measurement.date >= start).all()
    session.close()
    temp_obs = {}
    temp_obs['Min_Temp']=start_end[0][0]
    temp_obs['avg_Temp']=start_end[0][1]
    temp_obs['max_Temp']=start_end[0][2]
    return jsonify(temp_obs)


if __name__ == '__main__' :
    app.run(debug=True)