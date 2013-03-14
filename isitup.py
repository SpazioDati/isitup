import urllib2
from contextlib import contextmanager
import uwsgi
from bottle import get, default_app, request, abort, response

EXPIRE = 60


def invalid():
    response.status = 400
    return 'URL is down'


@contextmanager
def lock(lock_idx):
    uwsgi.lock(lock_idx)
    yield
    uwsgi.unlock(lock_idx)


@get('/')
def index():
    try:
        url = request.params['url']
    except KeyError:
        abort(400, 'Please provide a url')

    url_hash = hash(url)
    cache_key = str(url_hash)

    lock_idx = url_hash % 24
    with lock(lock_idx):
        cache = uwsgi.cache_get(cache_key)
        if cache == 'd':
            return invalid()
        if cache == 'u':
            return url

        try:
            urllib2.urlopen(url, timeout=10)
        except:
            uwsgi.cache_set(cache_key, 'd', EXPIRE)
            return invalid()
        else:
            uwsgi.cache_set(cache_key, 'u', EXPIRE)
            return url

application = default_app()
