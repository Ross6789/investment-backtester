from flask import Flask, request, jsonify
from flask_cors import CORS
from api_runner import run_backtest

app = Flask(__name__)
CORS(app)

@app.route('/api/run-backtest', methods=['POST'])
def backtest_api():
    try:
        data = request.get_json()
        result = run_backtest(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
