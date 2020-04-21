from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import parse_qs, urlparse

from elasticsearch import Elasticsearch


class SearchApi:
    """
    Basic search api for elasticsearch service.
    """

    ES_HOST = "elasticsearch:9200"
    INDEX_NAME = "wikis"
    WIKIPIPEDIA = "https://en.wikipedia.org"

    def __init__(self, path):
        parse_url = urlparse(path)
        query_prms = parse_qs(parse_url.query)
        self.path = parse_url.path.rstrip("/")
        self.srch_val = query_prms["q"][0] if "q" in query_prms else ""
        self.list_val = query_prms["list"][0] if "list" in query_prms else ""
        self.es = Elasticsearch(self.ES_HOST, index=self.INDEX_NAME)

    def get_query_dsl(self):
        """
        Get raw query_dsl based on url query paramters.
        """
        multi_match = {
            "multi_match": {
                "query": self.srch_val,
                "fields": ["title", "content"]
            }
        }

        if self.list_val:
            query_dsl = {"query": {
                "bool": {
                    "must": [
                        multi_match,
                        {
                            "match_phrase": {
                                "list": self.list_val
                            }
                        }
                    ],
                }
            }}
        else:
            query_dsl = {"query": multi_match}

        return query_dsl

    def get_health_status(self):
        """
        Get the elasticsearch cluster's health status.
        """
        health = self.es.cluster.health()
        return health["status"]

    def is_valid_route(self):
        """
        Validate if path is a valid route.
        """
        status = False
        if self.path in ["/search/wikis", "/status"]:
            status = True
        return status

    def make_resource(self, result):
        """
        Create the resource for given result.
        """
        num_of_results = result["hits"]["total"]["value"]
        data = {"number_of_results": num_of_results, "results": []}
        if num_of_results:
            for item in result["hits"]["hits"]:
                source = item["_source"]
                data["results"].append({
                    "title": source["title"],
                    "link": self.WIKIPIPEDIA + source["link"],
                })
        return data

    def run_query(self):
        """
        Execute the search query and return the data.
        """
        query = self.get_query_dsl()

        result = self.es.search(
            body=query,
            _source=["title", "link"]
        )

        data = self.make_resource(result)
        return data


class ApiHandler(BaseHTTPRequestHandler):
    """
    Basic HTTP request handler for search api.
    """

    def do_GET(self):
        """
        Handle GET request.
        """
        api = SearchApi(self.path) # Initialize the api.
        if api.is_valid_route(): # Only valid routes are served 200.
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            if api.path == "/status":
                data = {"status": api.get_health_status()}
            else:
                # Search criteria is empty - serve 400.
                if not api.srch_val:
                    self.send_error(400, "Missing required query param 'q'.")
                data = api.run_query()
            self.wfile.write(json.dumps(data).encode()) # Success
        else:
            self.send_error(404)


with HTTPServer(("", 9250), ApiHandler) as httpd:
    httpd.serve_forever()
