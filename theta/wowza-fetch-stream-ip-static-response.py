import json
from flask import jsonify
from flask import Flask, Response

app = Flask(__name__)

@app.route("/get_streamer_ip/")
def get_login_data():

    projresponse = []
    status_code = {"status_code": 200}
    stream_ip = {"stream_ip" : "101.123.9.178"}
    stream_port = {"stream_port" : "1935"}
    stream_start_time = {"stream_start_time" : "25 March 2020"}
    stream_name = {"stream_name" : "stream-1f"}
    stream_status = {"stream_status" : "Started"}
    user = {"user" : "demo-user"}
    password = {"password" : "demo-pass"}
    projresponse.append(status_code)
    projresponse.append(stream_ip)
    projresponse.append(stream_port)
    projresponse.append(stream_start_time)
    projresponse.append(stream_name)
    projresponse.append(stream_status)
    projresponse.append(user)
    projresponse.append(password)
    #a = {"status_code":"200", "stream_ip": "101.123.9.178", "stream_port":"1935", "stream_start_time": "25 March 2020","stream_name": "stream-1f","stream_status": "Started","user": "demo-user","password": "demo-pass"}
    # a python dictionary
    #projresponse = json.dumps(a)
    respjson = {
        "status_code": 200,
        "stream_ip" : "101.123.9.178",
        "stream_port" : "1935",
        "stream_start_time" : "25 March 2020",
        "stream_name" : "stream-1f",
        "stream_status" : "Started",
        "user" : "demo-user",
        "password" : "demo-pass"
        }

    response = jsonify(respjson)
    response.headers.set("Access-Control-Allow-Origin", "*")
    response.headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    response.headers.set("Content-Type", "application/json")
    return response

if __name__ == "__main__":
    app.run(debug=True)