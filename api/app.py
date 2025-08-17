from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from backend.backtest.data_cache import preload_all_data
from .run_backtest import async_run_backtest
from backend.core.paths import get_asset_metadata_json_path
from threading import Thread
import traceback, os, uuid, traceback
from dotenv import load_dotenv



app = Flask(__name__,static_folder="static")
CORS(app)

load_dotenv()  # loads environment variables

# Set app mode based on environment variable
dev_mode = os.getenv("DEV_MODE","false").lower() == "true"

# Load and cache backtest data on start up
preload_all_data(dev_mode)    

jobs = {}

@app.route("/api/run-backtest", methods=["POST"])
def start_backtest():
    payload = request.get_json()
    job_id = str(uuid.uuid4())
    
    # Create initial job entry
    jobs[job_id] = {"status": "running", "result": None}

    # Run backtest in a separate thread
    Thread(target=async_run_backtest, args=(jobs,job_id, payload, dev_mode)).start()

    return jsonify({"job_id": job_id})


@app.route("/api/backtest-status/<job_id>")
def backtest_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)


@app.route("/api/assets")
def get_assets():
    return send_file(get_asset_metadata_json_path(dev_mode), mimetype='application/json')


@app.route("/api/download-report")
def download_report():
    file_path = request.args.get("file")

    if not file_path:
        return "Missing file parameter", 400

    if not os.path.exists(file_path):
        return "File not found or expired", 404

    return send_file(file_path, as_attachment=True)


if __name__ == '__main__':
    app.run(port=5002,debug=True, use_reloader = False)