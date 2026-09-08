#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

#include <string>
#include <vector>
#include <atomic>
#include <stdint.h>


/// A small blocking HTTP/1.1 server built directly on POSIX sockets.
///
/// It exists so that the database can be spoken to over HTTP without the project
/// taking on a dependency: nothing here needs anything beyond libc, pthreads and
/// the POSIX socket API, so it cross compiles for arm, arm64 and x86 without any
/// library having an opinion about it.
///
/// It is deliberately small, which means it is not a general purpose web server:
/// there is no chunked transfer encoding, no TLS, no compression and no static file
/// serving. It is intended to sit on a trusted interface or behind a reverse proxy
/// that terminates TLS.


typedef struct {
    std::string name;   // lowercased
    std::string value;
} HTTP_HEADER;


typedef struct {
    std::string method;
    std::string path;                        // percent decoded, no query string
    std::string query;                       // raw, still encoded
    std::string version;                     // "HTTP/1.1" or "HTTP/1.0"
    std::string body;
    std::vector<HTTP_HEADER> headers;
    std::string client_address;
} HTTP_REQUEST;


/// Writes a response out as it is produced instead of assembling it in memory.
///
/// A handler that knows its response could be large calls begin() and then write()
/// as many times as it needs. Nothing beyond one buffer's worth is ever held, so
/// the peak memory of a request stops depending on the size of its answer.
///
/// The status and headers go out with begin(), so everything that decides them has
/// to be settled first: once streaming has started there is no way to turn the
/// response into an error.
///
/// Framing is chunked for HTTP/1.1. An HTTP/1.0 client gets a close delimited body
/// instead, since it cannot be expected to understand chunked encoding, and the
/// connection is not reused in that case.

class HttpStream
{
public:
    HttpStream(int socket_fd, const std::string &http_version, bool keep_alive);

    void begin(int status, const char *content_type, const std::vector<HTTP_HEADER> &headers);

    bool write(const char *data, size_t length);
    bool write(const std::string &text);
    void finish();

    bool started() const { return begun; }
    bool failed() const { return broken; }
    bool keepAlive() const { return keep_alive; }

private:
    bool flushBuffer();
    bool sendAll(const char *data, size_t length);

    int socket_fd;
    bool chunked;
    bool keep_alive;
    bool begun;
    bool finished;
    bool broken;
    std::string buffer;
};


typedef struct {
    int status = 0;
    std::string content_type;
    std::string body;
    std::vector<HTTP_HEADER> headers;        // extra response headers

    /// Available to every handler. Fill in body for an ordinary response, or use
    /// this to stream one; doing both is a mistake and the buffered body is
    /// ignored once streaming has started.
    HttpStream *stream = NULL;
} HTTP_RESPONSE;


/// Returns the value of a request header, or an empty string. Names are matched
/// case insensitively.
std::string httpHeader(const HTTP_REQUEST &request, const char *name);

/// Returns a query string parameter, percent decoded, or an empty string.
/// found is set when the parameter was present at all, so an empty value and an
/// absent one can be told apart.
std::string httpQueryParam(const HTTP_REQUEST &request, const char *name, bool *found = NULL);

/// Percent decoding, with + meaning space.
std::string httpUrlDecode(const std::string &text);

/// The reason phrase for a status code.
const char *httpStatusText(int status);


/// Called for every request. Fill in response; anything you do not set gets a
/// sensible default. Handlers run on their own thread and may run concurrently,
/// so whatever context points at has to be safe to use from several threads.

typedef void (*HTTP_HANDLER)(const HTTP_REQUEST &request, HTTP_RESPONSE &response, void *context);


class HttpServer
{
public:
    HttpServer();
    ~HttpServer();

    /// Binds and listens. Returns false and logs on failure.
    bool start(const char *bind_address, int port);

    /// Accepts connections until stop() is called. Blocks.
    void run(HTTP_HANDLER handler, void *context);

    /// Asks run() to return. Safe to call from a signal handler or another thread.
    void stop();

    /// The port actually bound, which is what the operating system chose when
    /// start() was given port 0. Returns -1 before start() succeeds.
    int boundPort() const;

    /// Tuning. Set before start(); the defaults suit an internal API.
    int max_connections;              // concurrent connections, further ones get 503
    int max_body_bytes;               // request body cap, larger gets 413
    int max_header_bytes;             // request head cap, larger gets 431
    int header_timeout_seconds;       // time allowed to send a request
    int idle_timeout_seconds;         // time a kept alive connection may sit idle
    int max_requests_per_connection;  // keep alive cap

    /// Atomic rather than volatile: stop() is called from a signal handler and
    /// from other threads while run() is reading this, which volatile does not
    /// make safe. std::atomic<bool> is lock free everywhere this builds, so it is
    /// also safe to set from a signal handler.
    std::atomic<bool> running;

    /// Serves one accepted connection until it closes. Public only because the
    /// per connection thread function calls it; not part of the API.
    void serveConnection(int client_socket, const std::string &client_address, HTTP_HANDLER handler, void *context);

private:
    int listen_socket;
};


#endif // HTTP_SERVER_H
