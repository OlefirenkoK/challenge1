from logging.config import dictConfig

from flask import Flask

from challenge.models import engine, Base


app = Flask(__name__)


SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8080


def init_db():
    # TODO: Move to the first migration
    Base.metadata.create_all(engine)


def run():
    from challenge import api

    app.register_blueprint(api.api_patients)
    app.register_blueprint(api.api_payments)

    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })
    app.run(host=SERVER_HOST, port=SERVER_PORT)
