from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import os, sys
os.chdir(os.path.dirname(__file__))  # serve files from THIS folder
class CORS(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges")
        super().end_headers()
if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    httpd = ThreadingHTTPServer(("127.0.0.1", port), CORS)
    print(f"Serving {os.getcwd()} at http://127.0.0.1:{port}")
    httpd.serve_forever()
