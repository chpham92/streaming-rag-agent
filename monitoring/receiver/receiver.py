"""Stands in for a real incident-response destination (PagerDuty, Slack,
Opsgenie) — logs every alert Alertmanager routes to it, firing and resolved,
so an alert's whole lifecycle is provable from this container's logs alone
rather than only from the Alertmanager UI's current-state view.
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(length) or b"{}")
        for alert in payload.get("alerts", []):
            print(json.dumps({
                "status": alert.get("status"),
                "alertname": alert.get("labels", {}).get("alertname"),
                "severity": alert.get("labels", {}).get("severity"),
                "summary": alert.get("annotations", {}).get("summary"),
                "description": alert.get("annotations", {}).get("description"),
                "startsAt": alert.get("startsAt"),
                "endsAt": alert.get("endsAt"),
            }), flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # the JSON above is the log; skip BaseHTTPRequestHandler's default access log noise


if __name__ == "__main__":
    print("alert-receiver listening on :9100/alert", flush=True)
    HTTPServer(("0.0.0.0", 9100), Handler).serve_forever()
