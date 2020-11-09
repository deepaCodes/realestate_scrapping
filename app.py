import json
import pathlib
from datetime import datetime

import pandas
from flask import Flask, jsonify
from flask_cors import CORS

from cloud.aws import get_scrapper_last_run_datetime, query_property_listing, DATA_TYPE_DICT

app = Flask(__name__)
CORS(app)

config = {'running': False}

CURRENT_DIR = pathlib.Path(__file__).parent.absolute()


# report_df = pandas.read_csv('{}/DATA/Aggregated_data.csv'.format(CURRENT_DIR))
# result_json = json.loads(report_df.to_json(orient="records"))


@app.route('/api/scrapper/home')
def hello():
    """
    http://localhost:8080/api/scrapper/home
    :return:
    """
    text = 'hello: {}'.format(datetime.now())
    print(text)
    return jsonify(hello=text)  # Returns HTTP Response with {"hello": "world"}


@app.route('/api/scrapper/lastrun')
def scrapper_last_run():
    """
    http://localhost:8080/api/scrapper/home
    :return:
    """
    response = get_scrapper_last_run_datetime()
    return jsonify(response)  # Returns HTTP Response with {"hello": "world"}

@app.route('/api/scrapper/report', methods=['POST'])
def get_report():
    print('fetching report. current time: {}'.format(datetime.now()))
    listings = query_property_listing()
    for row in listings:
        for k, v in DATA_TYPE_DICT.items():
            if v == 'int' and row[k]:
                row[k] = int(float(row[k]))
            elif v == 'float' and row[k]:
                row[k] = float(row[k])

    return jsonify(data=listings)


if __name__ == '__main__':
    # ipv4 ip: 192.168.1.152
    # app.run()
    public_ip = '0.0.0.0'
    app.run(host=public_ip, port=8080)
