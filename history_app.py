from flask import Flask

from config import ConnectionParams


app = Flask("mbta-history")
app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://{user}:{password}@{host}:5432/{dbname}".format(**ConnectionParams)
db = SQLAlchemy(app)


class VehicleUpdate(db.Model):
    trip_id = db.Column(db.String(50))
    trip_start = db.Column(db.Date())
    stop_id = db.Column(db.String(50))
    stop_sequence = db.Column(db.Integer())
    vehicle_id = db.Column(db.DateTime(timezone=True))
    status = db.Column(db.DateTime(timezone=True))
    timestamp = db.Column(db.DateTime(timezone=True))
    lat = db.Column(db.Float())
    lon = db.Column(db.Float())


# Supported:
# Specify data format
# Filters: date range(s), vehicle ids

@app.route("/")
def app_index():
    return "Hello"


if __name__ == "__main__":
    app.run()
