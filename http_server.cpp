#include "http_server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <atomic>
#include <thread>

#include "debug.h"


static std::atomic<int> g_active_connections(0);


HttpServer::HttpServer()
{
    max_connections = 64;
    max_body_bytes = 1024 * 1024;
    max_header_bytes = 16 * 1024;
    header_timeout_seconds = 15;
    idle_timeout_seconds = 5;
    max_requests_per_connection = 100;

    running.store(false);
    listen_socket = -1;
}


HttpServer::~HttpServer()
{
    if(listen_socket >= 0)
        ::close(listen_socket);
}


const char *httpStatusText(int status){

    switch(status){
        case 200: return "OK";
        case 201: return "Created";
        case 204: return "No Content";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 409: return "Conflict";
        case 411: return "Length Required";
        case 413: return "Payload Too Large";
        case 414: return "URI Too Long";
        case 422: return "Unprocessable Entity";
        case 429: return "Too Many Requests";
        case 431: return "Request Header Fields Too Large";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 503: return "Service Unavailable";
        default:  return "Error";
    }
}


std::string httpUrlDecode(const std::string &text){

    std::string out;
    out.reserve(text.size());

    for(size_t i = 0; i < text.size(); i++){

        char c = text[i];

        if(c == '+'){
            out += ' ';
            continue;
        }

        if(c == '%' && i + 2 < text.size()){

            char hex[3] = { text[i+1], text[i+2], 0 };
            char *end = NULL;
            long value = strtol(hex,&end,16);

            if(end == hex + 2){
                out += (char)value;
                i += 2;
                continue;
            }
        }

        out += c;
    }

    return out;
}


std::string httpHeader(const HTTP_REQUEST &request, const char *name){

    std::string wanted(name);
    for(size_t i = 0; i < wanted.size(); i++)
        wanted[i] = (char)tolower((unsigned char)wanted[i]);

    for(size_t i = 0; i < request.headers.size(); i++){
        if(request.headers[i].name == wanted)
            return request.headers[i].value;
    }

    return std::string();
}


std::string httpQueryParam(const HTTP_REQUEST &request, const char *name, bool *found){

    if(found != NULL)
        *found = false;

    const std::string &query = request.query;
    size_t wanted_length = strlen(name);
    size_t position = 0;

    while(position < query.size()){

        size_t separator = query.find('&',position);
        if(separator == std::string::npos)
            separator = query.size();

        size_t equals = query.find('=',position);

        if(equals != std::string::npos && equals < separator){

            if(equals - position == wanted_length && query.compare(position,wanted_length,name) == 0){
                if(found != NULL)
                    *found = true;
                return httpUrlDecode(query.substr(equals + 1, separator - equals - 1));
            }

        } else if(separator - position == wanted_length && query.compare(position,wanted_length,name) == 0){

            // present with no value at all
            if(found != NULL)
                *found = true;
            return std::string();
        }

        position = separator + 1;
    }

    return std::string();
}


/// Reads until the terminating blank line of the request head, or until the cap is
/// hit. Anything read past the head is left in leftover, which is where a pipelined
/// body (and the next request on a kept alive connection) shows up.

static bool readRequestHead(int socket_fd, std::string &head, std::string &leftover, int max_bytes){

    char buffer[4096];

    // whatever the previous request left behind may already contain the whole head
    head = leftover;
    leftover.clear();

    for(;;){

        size_t terminator = head.find("\r\n\r\n");

        if(terminator != std::string::npos){
            leftover = head.substr(terminator + 4);
            head.erase(terminator + 4);
            return true;
        }

        if((int)head.size() > max_bytes)
            return false;

        ssize_t got = recv(socket_fd,buffer,sizeof(buffer),0);

        if(got <= 0)
            return false;

        head.append(buffer,(size_t)got);
    }
}


static bool parseRequestHead(const std::string &head, HTTP_REQUEST &request){

    size_t line_end = head.find("\r\n");
    if(line_end == std::string::npos)
        return false;

    std::string request_line = head.substr(0,line_end);

    size_t first_space = request_line.find(' ');
    if(first_space == std::string::npos)
        return false;

    size_t second_space = request_line.find(' ',first_space + 1);
    if(second_space == std::string::npos)
        return false;

    request.method = request_line.substr(0,first_space);
    request.version = request_line.substr(second_space + 1);

    std::string target = request_line.substr(first_space + 1, second_space - first_space - 1);

    size_t question = target.find('?');

    if(question == std::string::npos){
        request.path = httpUrlDecode(target);
        request.query.clear();
    } else {
        request.path = httpUrlDecode(target.substr(0,question));
        request.query = target.substr(question + 1);
    }

    size_t position = line_end + 2;

    while(position < head.size()){

        size_t end = head.find("\r\n",position);

        if(end == std::string::npos || end == position)
            break; // blank line, end of head

        std::string line = head.substr(position,end - position);
        position = end + 2;

        size_t colon = line.find(':');
        if(colon == std::string::npos)
            continue;

        HTTP_HEADER header;
        header.name = line.substr(0,colon);

        for(size_t i = 0; i < header.name.size(); i++)
            header.name[i] = (char)tolower((unsigned char)header.name[i]);

        size_t value_start = colon + 1;
        while(value_start < line.size() && (line[value_start] == ' ' || line[value_start] == '\t'))
            value_start++;

        header.value = line.substr(value_start);

        while(!header.value.empty() && (header.value[header.value.size()-1] == ' ' || header.value[header.value.size()-1] == '\t'))
            header.value.erase(header.value.size()-1);

        // A duplicated header is a request smuggling shape, not something to guess at
        for(size_t i = 0; i < request.headers.size(); i++){
            if(request.headers[i].name == header.name && header.name == "content-length")
                return false;
        }

        request.headers.push_back(header);
    }

    return true;
}


#define HTTP_STREAM_BUFFER 65536


HttpStream::HttpStream(int fd, const std::string &http_version, bool allow_keep_alive)
{
    socket_fd = fd;

    // Only HTTP/1.1 can be sent chunked. A 1.0 client gets a close delimited body,
    // which means the connection cannot then be reused.
    chunked = (http_version == "HTTP/1.1");
    keep_alive = allow_keep_alive && chunked;

    begun = false;
    finished = false;
    broken = false;
}


bool HttpStream::sendAll(const char *data, size_t length){

    size_t sent = 0;

    while(sent < length){

        ssize_t wrote = send(socket_fd,data + sent,length - sent,0);

        if(wrote <= 0){
            broken = true;
            return false;
        }

        sent += (size_t)wrote;
    }

    return true;
}


void HttpStream::begin(int status, const char *content_type, const std::vector<HTTP_HEADER> &headers){

    if(begun || broken)
        return;

    begun = true;

    char head[1024];

    int length = snprintf(head,sizeof(head),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Connection: %s\r\n",
        status, httpStatusText(status),
        content_type ? content_type : "application/json",
        keep_alive ? "keep-alive" : "close");

    if(length < 0 || length >= (int)sizeof(head)){
        broken = true;
        return;
    }

    std::string out(head,(size_t)length);

    if(chunked)
        out += "Transfer-Encoding: chunked\r\n";

    for(size_t i = 0; i < headers.size(); i++){
        // a header value carrying CR or LF would let a handler forge a response
        if(headers[i].value.find_first_of("\r\n") != std::string::npos)
            continue;
        out += headers[i].name + ": " + headers[i].value + "\r\n";
    }

    out += "\r\n";

    sendAll(out.data(),out.size());
}


bool HttpStream::flushBuffer(){

    if(buffer.empty())
        return !broken;

    if(chunked){

        char size_line[32];
        int length = snprintf(size_line,sizeof(size_line),"%lx\r\n",(unsigned long)buffer.size());

        if(!sendAll(size_line,(size_t)length))
            return false;

        if(!sendAll(buffer.data(),buffer.size()))
            return false;

        if(!sendAll("\r\n",2))
            return false;

    } else if(!sendAll(buffer.data(),buffer.size())){
        return false;
    }

    buffer.clear();
    return true;
}


bool HttpStream::write(const char *data, size_t length){

    if(broken || !begun)
        return false;

    buffer.append(data,length);

    if(buffer.size() >= HTTP_STREAM_BUFFER)
        return flushBuffer();

    return true;
}


bool HttpStream::write(const std::string &text){
    return write(text.data(),text.size());
}


void HttpStream::finish(){

    if(!begun || finished)
        return;

    finished = true;

    if(!flushBuffer())
        return;

    if(chunked)
        sendAll("0\r\n\r\n",5);
}


static void writeResponse(int socket_fd, HTTP_RESPONSE &response, bool keep_alive){

    if(response.status == 0)
        response.status = 200;

    if(response.content_type.empty())
        response.content_type = "application/json";

    char head[1024];

    int length = snprintf(head,sizeof(head),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %lu\r\n"
        "Connection: %s\r\n",
        response.status, httpStatusText(response.status),
        response.content_type.c_str(),
        (unsigned long)response.body.size(),
        keep_alive ? "keep-alive" : "close");

    if(length < 0 || length >= (int)sizeof(head))
        return;

    std::string out(head,(size_t)length);

    for(size_t i = 0; i < response.headers.size(); i++){
        // a header value carrying CR or LF would let a handler forge a response
        if(response.headers[i].value.find_first_of("\r\n") != std::string::npos)
            continue;
        out += response.headers[i].name + ": " + response.headers[i].value + "\r\n";
    }

    out += "\r\n";
    out += response.body;

    size_t sent = 0;

    while(sent < out.size()){
        ssize_t wrote = send(socket_fd,out.data() + sent,out.size() - sent,0);
        if(wrote <= 0)
            return;
        sent += (size_t)wrote;
    }
}


static void setSocketTimeout(int socket_fd, int seconds){

    struct timeval timeout;
    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    setsockopt(socket_fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
    setsockopt(socket_fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
}


void HttpServer::serveConnection(int client_socket, const std::string &client_address, HTTP_HANDLER handler, void *context){

    std::string leftover;

    for(int served = 0; served < max_requests_per_connection; served++){

        setSocketTimeout(client_socket, served == 0 ? header_timeout_seconds : idle_timeout_seconds);

        std::string head;

        if(!readRequestHead(client_socket,head,leftover,max_header_bytes)){

            if(head.size() > 0 && (int)head.size() > max_header_bytes){
                HTTP_RESPONSE response;
                response.status = 431;
                response.body = "{\"error\":\"header_too_large\"}";
                writeResponse(client_socket,response,false);
            }

            return; // client went away, timed out, or sent nothing more
        }

        HTTP_REQUEST request;
        request.client_address = client_address;

        if(!parseRequestHead(head,request)){
            HTTP_RESPONSE response;
            response.status = 400;
            response.body = "{\"error\":\"bad_request\"}";
            writeResponse(client_socket,response,false);
            return;
        }

        setSocketTimeout(client_socket,header_timeout_seconds);

        // Chunked bodies are not supported. Saying so is safer than guessing at the
        // framing, which is how request smuggling bugs happen.
        std::string transfer_encoding = httpHeader(request,"transfer-encoding");

        if(!transfer_encoding.empty()){
            HTTP_RESPONSE response;
            response.status = 501;
            response.body = "{\"error\":\"transfer_encoding_unsupported\",\"message\":\"send a body with Content-Length\"}";
            writeResponse(client_socket,response,false);
            return;
        }

        std::string content_length_text = httpHeader(request,"content-length");
        long content_length = 0;

        if(!content_length_text.empty()){

            char *end = NULL;
            content_length = strtol(content_length_text.c_str(),&end,10);

            if(end == content_length_text.c_str() || *end != 0 || content_length < 0){
                HTTP_RESPONSE response;
                response.status = 400;
                response.body = "{\"error\":\"bad_content_length\"}";
                writeResponse(client_socket,response,false);
                return;
            }

            if(content_length > max_body_bytes){
                HTTP_RESPONSE response;
                response.status = 413;
                response.body = "{\"error\":\"body_too_large\"}";
                writeResponse(client_socket,response,false);
                return; // the unread body would desync a kept alive connection
            }
        }

        request.body = leftover.substr(0,(size_t)content_length < leftover.size() ? (size_t)content_length : leftover.size());
        leftover.erase(0,request.body.size());

        while((long)request.body.size() < content_length){

            char buffer[4096];
            size_t want = (size_t)content_length - request.body.size();
            if(want > sizeof(buffer))
                want = sizeof(buffer);

            ssize_t got = recv(client_socket,buffer,want,0);

            if(got <= 0)
                return;

            request.body.append(buffer,(size_t)got);
        }

        // Decided before the handler runs, because a streaming handler sends the
        // response head itself and needs to know what to put in Connection.
        std::string connection_header = httpHeader(request,"connection");
        bool keep_alive = true;

        for(size_t i = 0; i < connection_header.size(); i++)
            connection_header[i] = (char)tolower((unsigned char)connection_header[i]);

        if(connection_header.find("close") != std::string::npos)
            keep_alive = false;

        if(served + 1 >= max_requests_per_connection)
            keep_alive = false;

        if(!running.load())
            keep_alive = false;

        if(request.version != "HTTP/1.1")
            keep_alive = false;

        HttpStream stream(client_socket,request.version,keep_alive);

        HTTP_RESPONSE response;
        response.status = 0;
        response.stream = &stream;

        handler(request,response,context);

        if(stream.started()){

            stream.finish();

            if(stream.failed() || !stream.keepAlive())
                return;

        } else {

            writeResponse(client_socket,response,keep_alive);

            if(!keep_alive)
                return;
        }
    }
}


bool HttpServer::start(const char *bind_address, int port){

    // Writing to a socket the peer has closed must not take the process down
    signal(SIGPIPE,SIG_IGN);

    char port_text[16];
    snprintf(port_text,sizeof(port_text),"%d",port);

    struct addrinfo hints;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;      // v4 or v6, whichever the address names
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo *results = NULL;
    int rc = getaddrinfo(bind_address,port_text,&hints,&results);

    if(rc != 0){
        _ERROR("Could not resolve %s: %s\n",bind_address,gai_strerror(rc));
        return false;
    }

    int opened = -1;

    for(struct addrinfo *candidate = results; candidate != NULL; candidate = candidate->ai_next){

        opened = socket(candidate->ai_family,candidate->ai_socktype,candidate->ai_protocol);

        if(opened < 0)
            continue;

        int on = 1;
        setsockopt(opened,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(on));

        if(bind(opened,candidate->ai_addr,candidate->ai_addrlen) == 0)
            break;

        ::close(opened);
        opened = -1;
    }

    freeaddrinfo(results);

    if(opened < 0){
        _ERROR("Could not bind %s port %d: %s\n",bind_address,port,strerror(errno));
        return false;
    }

    if(listen(opened,64) != 0){
        _ERROR("Could not listen on %s port %d: %s\n",bind_address,port,strerror(errno));
        ::close(opened);
        return false;
    }

    listen_socket = opened;
    running.store(true);

    return true;
}


void HttpServer::stop(){
    running.store(false);
}


int HttpServer::boundPort() const {

    if(listen_socket < 0)
        return -1;

    struct sockaddr_storage bound;
    socklen_t length = sizeof(bound);

    if(getsockname(listen_socket,(struct sockaddr*)&bound,&length) != 0)
        return -1;

    if(bound.ss_family == AF_INET)
        return ntohs(((struct sockaddr_in*)&bound)->sin_port);

    if(bound.ss_family == AF_INET6)
        return ntohs(((struct sockaddr_in6*)&bound)->sin6_port);

    return -1;
}


struct HttpConnectionArgs {
    HttpServer *server;
    int client_socket;
    std::string client_address;
    HTTP_HANDLER handler;
    void *context;
};


static void connectionThread(HttpConnectionArgs *args){

    args->server->serveConnection(args->client_socket,args->client_address,args->handler,args->context);

    ::close(args->client_socket);
    delete args;

    g_active_connections--;
}


void HttpServer::run(HTTP_HANDLER handler, void *context){

    while(running.load()){

        // poll rather than a blocking accept, so stop() is noticed promptly
        struct pollfd waiting;
        waiting.fd = listen_socket;
        waiting.events = POLLIN;
        waiting.revents = 0;

        int ready = poll(&waiting,1,200);

        if(ready <= 0){
            if(ready < 0 && errno != EINTR)
                break;
            continue;
        }

        struct sockaddr_storage peer;
        socklen_t peer_length = sizeof(peer);

        int client_socket = accept(listen_socket,(struct sockaddr*)&peer,&peer_length);

        if(client_socket < 0)
            continue;

        char host[NI_MAXHOST] = "";
        getnameinfo((struct sockaddr*)&peer,peer_length,host,sizeof(host),NULL,0,NI_NUMERICHOST);

        if(g_active_connections.load() >= max_connections){

            HTTP_RESPONSE response;
            response.status = 503;
            response.body = "{\"error\":\"too_many_connections\"}";
            writeResponse(client_socket,response,false);
            ::close(client_socket);
            continue;
        }

        int on = 1;
        setsockopt(client_socket,IPPROTO_TCP,TCP_NODELAY,&on,sizeof(on));

        g_active_connections++;

        HttpConnectionArgs *args = new HttpConnectionArgs;
        args->server = this;
        args->client_socket = client_socket;
        args->client_address = host;
        args->handler = handler;
        args->context = context;

        try {
            std::thread(connectionThread,args).detach();
        } catch(...){
            g_active_connections--;
            ::close(client_socket);
            delete args;
        }
    }

    ::close(listen_socket);
    listen_socket = -1;

    // Let connections in flight finish before the caller tears the database down
    for(int waited = 0; waited < 100 && g_active_connections.load() > 0; waited++){
        struct timespec pause = { 0, 100 * 1000 * 1000 };
        nanosleep(&pause,NULL);
    }
}
