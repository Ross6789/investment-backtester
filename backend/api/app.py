from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from .run_backtest import run_backtest
from backend.core.paths import get_asset_metadata_json_path
import traceback
import os
import tempfile

app = Flask(__name__,static_folder="static")
CORS(app)

## Before trying excel download
# @app.route('/api/run-backtest', methods=['POST'])
# def backtest_api():
#     try:
#         data = request.get_json()
#         payload = run_backtest(data)
#         return jsonify({"success": True,**payload})
#     except Exception as e:
#         print(traceback.format_exc())  # prints full traceback in terminal
#         return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/run-backtest', methods=['POST'])
def backtest_api():
    try:
        payload = request.get_json()

        # Run backtest with input settings
        backtest_result, temp_excel_path = run_backtest(payload)

        # Create full download link (API call) using excel path
        download_link = f"/api/download-report?file={temp_excel_path}" if temp_excel_path else None

        return jsonify({"success": True,"excel_download": download_link,**backtest_result})
    except Exception as e:
        print(traceback.format_exc())  # prints full traceback in terminal
        return jsonify({"success": False, "error": str(e)}), 400
    
    
@app.route("/api/assets")
def get_assets():
    return send_file(get_asset_metadata_json_path(), mimetype='application/json')


@app.route("/api/download-report")
def download_report():
    file_path = request.args.get("file")

    if not file_path:
        return "Missing file parameter", 400

    if not os.path.exists(file_path):
        return "File not found or expired", 404

    return send_file(file_path, as_attachment=True)


if __name__ == '__main__':
    app.run(port=5002,debug=True)