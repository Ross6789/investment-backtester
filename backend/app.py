from flask import Flask, request, jsonify
from flask_cors import CORS
from .api_runner import run_backtest
import traceback

app = Flask(__name__)
CORS(app)


@app.route('/run-backtest', methods=['POST'])
def backtest_api():
    try:
        data = request.get_json()
        results = run_backtest(data)
        return jsonify({"success": True, "results":results})
    except Exception as e:
        print(traceback.format_exc())  # prints full traceback in terminal
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
