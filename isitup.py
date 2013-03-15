import json
import urllib
import urllib2
from contextlib import contextmanager
from functools import partial
from multiprocessing.dummy import Pool
import uwsgi
from bottle import get, default_app, request, abort, response, route

EXPIRE = 60
USERAGENT = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"


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
            req = urllib2.Request(
                url,
                headers={'User-Agent': USERAGENT}
            )
            urllib2.urlopen(req, timeout=10)
        except Exception, e:
            uwsgi.cache_set(cache_key, 'd', EXPIRE)
            return invalid()
        else:
            uwsgi.cache_set(cache_key, 'u', EXPIRE)
            return url


#### reconciliation
METADATA = {
    "name": "IsItUp? Reconciliation Service",
    "defaultTypes": [],
}


def search(host, query):
    # Example match format:
    """
    match = {
        "id": 'term id',
        "name": 'term label',
        "score": 50,
        "match": False, # Return True for exact match.
        "type": [{"id": "/base/oceanography/research_cruise",
                  "name": "Research Cruise"}]
        }
    matches.append(match)
    """
    url = 'http://{}/?{}'.format(host, urllib.urlencode(
        {'url': query.encode('utf8')})
    )

    try:
        urllib2.urlopen(url)
    except Exception, e:
        return []
    else:
        return [{
            'id': query,
            'name': query,
            'match': True,
            'score': 100,
            'type': [{
                'id': '/',
                'name': 'Basic service',
            }]
        }]


def jsonpify(obj):
    """
    Like jsonify but wraps result in a JSONP callback if a 'callback'
    query param is supplied.
    """
    try:
        callback = request.params['callback']
    except KeyError:
        content = json.dumps(obj)
    else:
        content = '{}({})'.format(callback, json.dumps(obj))

    response.content_type = "text/javascript"
    return content


@route('/reconcile', method=['GET', 'POST'])
def reconcile():
    host = request.headers['Host']

    # If a single 'query' is provided do a straightforward search.
    query = request.params.get('query')
    if query:
        # If the 'query' param starts with a "{" then it is a JSON object
        # with the search string as the 'query' member. Otherwise,
        # the 'query' param is the search string itself.
        if query.startswith("{"):
            query = json.loads(query)['query']
        results = search(host, query)
        return jsonpify({"result": results})

    # If a 'queries' parameter is supplied then it is a dictionary
    # of (key, query) pairs representing a batch of queries. We
    # should return a dictionary of (key, results) pairs.
    queries = request.params.get('queries')
    if queries:
        queries = json.loads(queries)

        def f(host, t):
            key, query = t
            return key, {"result": search(host, query['query'])}

        pool = Pool(10)

        f = partial(f, host)

        results = dict(pool.map(f, queries.items()))
        return jsonpify(results)

    # If neither a 'query' nor 'queries' parameter is supplied then
    # we should return the service metadata.
    return jsonpify(METADATA)

application = default_app()
