from flask import Flask, request, jsonify
from flask_cors import CORS
from .api.run_backtest import run_backtest
import traceback

app = Flask(__name__)
CORS(app)

@app.route('/api/run-backtest', methods=['POST'])
def backtest_api():
    try:
        data = request.get_json()
        results = run_backtest(data)
        return jsonify({"success": True, "results":results})
    except Exception as e:
        print(traceback.format_exc())  # prints full traceback in terminal
        return jsonify({"success": False, "error": str(e)}), 400


# @app.route('/api/run-backtest-test', methods=['GET'])
# def api_run():
#     return jsonify({"success": True, "results":"my test"})


if __name__ == '__main__':
    app.run(port=5002,debug=True)
