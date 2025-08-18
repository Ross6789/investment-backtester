from flask import Flask, request, jsonify, send_file, send_from_directory
from backend.backtest.data_cache import preload_all_data
from .run_backtest import async_run_backtest
from backend.core.paths import get_asset_metadata_json_path
from threading import Thread
import traceback, os, uuid, traceback
from dotenv import load_dotenv
import pathlib

BASE_DIR = pathlib.Path(__file__).parent.resolve()
FRONTEND_DIST = BASE_DIR / "frontend_dist"

app = Flask(__name__,static_folder=str(FRONTEND_DIST), static_url_path="")

# loads environment variables to find which mdoe to run (devolpment vs production)
load_dotenv() 
dev_mode = os.getenv("DEV_MODE","false").lower() == "true"

# Load and cache backtest data on start up
preload_all_data(dev_mode)    
jobs = {}


# --- API ROUTES --- #

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


# --- REACT ROUTES --- #

@app.route("/", defaults={"path": ""})

@app.route("/<path:path>")
def serve_frontend(path):
    """Serve React app for any non-API route"""
    # If the requested file exists in frontend_dist, serve it
    file_path = FRONTEND_DIST / path
    if file_path.exists() and file_path.is_file():
        return send_from_directory(FRONTEND_DIST, path)
    # Otherwise serve index.html so React handles routing
    return send_from_directory(FRONTEND_DIST, "index.html")

# Fall back to index.html if 404 error : ie. user refreshes page while on settings or results
@app.errorhandler(404)
def not_found(e):
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app.run(port=5001,debug=True, use_reloader = False)