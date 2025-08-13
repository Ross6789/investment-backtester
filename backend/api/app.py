from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from .run_backtest import run_backtest
from backend.core.paths import get_asset_metadata_json_path
import traceback

app = Flask(__name__,static_folder="static")
CORS(app)

@app.route('/api/run-backtest', methods=['POST'])
def backtest_api():
    try:
        data = request.get_json()
        payload = run_backtest(data)
        return jsonify({"success": True,**payload})
    except Exception as e:
        print(traceback.format_exc())  # prints full traceback in terminal
        return jsonify({"success": False, "error": str(e)}), 400
    
@app.route("/api/assets")
def get_assets():
    return send_file(get_asset_metadata_json_path(), mimetype='application/json')


if __name__ == '__main__':
    app.run(port=5002,debug=True)
