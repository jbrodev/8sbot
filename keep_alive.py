import os
import threading

from flask import Flask


app = Flask(__name__)


@app.get("/")
def index():
    return "ok"


def keep_alive():
    port = int(os.getenv("PORT", "8080"))

    def run():
        app.run(host="0.0.0.0", port=port)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
