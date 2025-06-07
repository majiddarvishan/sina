#include "turbonet_server.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <cstring>
#include <iostream>

namespace turbonet {

// ----- Session Implementation -----
TurboNetServer::Session::Session(boost::asio::ip::tcp::socket sock,
                                 TurboNetServer& server)
    : socket(std::move(sock)),
      strand_(socket.get_executor()),   // executor_type matches socket
      serverRef(server)
{}

void TurboNetServer::Session::start() {
    doReadHeader();
}

void TurboNetServer::Session::doReadHeader() {
    auto self = shared_from_this();
    boost::asio::async_read(socket,
        boost::asio::buffer(headerBuf),
        boost::asio::bind_executor(strand_,
            [this, self](auto ec, auto len){ onReadHeader(ec,len); }));
}

void TurboNetServer::Session::onReadHeader(const boost::system::error_code& ec, std::size_t) {
    if (ec) { serverRef.removeSession(shared_from_this()); return; }
    uint32_t total = ntohl(*reinterpret_cast<uint32_t*>(headerBuf.data()));
    uint8_t pid = headerBuf[4];
    uint8_t status = headerBuf[5];
    uint32_t seq = ntohl(*reinterpret_cast<uint32_t*>(headerBuf.data()+6));
    uint32_t bodyLen = total - 10;
    doReadBody(bodyLen);
}

void TurboNetServer::Session::doReadBody(uint32_t len) {
    bodyBuf.resize(len);
    auto self = shared_from_this();
    boost::asio::async_read(socket,
        boost::asio::buffer(bodyBuf),
        boost::asio::bind_executor(strand_,
            [this, self](auto ec, auto l){ onReadBody(ec,l); }));
}

void TurboNetServer::Session::onReadBody(const boost::system::error_code& ec, std::size_t) {
    if (ec) { serverRef.removeSession(shared_from_this()); return; }

    // Handle bind request
    if (!authenticated && headerBuf[4] == 0x01) {
        std::string cid(bodyBuf.begin(), bodyBuf.end());
        if (serverRef.onAuth_ && serverRef.onAuth_(cid)) {
            authenticated = true;
            // Send bind-resp
            std::string id = "test_server";
            std::vector<uint8_t> resp(10 + id.length());
            uint32_t be = htonl(10 + id.length());
            std::memcpy(resp.data(), &be, 4);
            resp[4] = 0x81;
            resp[5] = 0x00; // success status
            uint32_t seq = ntohl(*reinterpret_cast<uint32_t*>(headerBuf.data()+6));
            uint32_t beSeq = htonl(seq);
            std::memcpy(resp.data()+6, &beSeq, 4);

            std::memcpy(resp.data() + 10, reinterpret_cast<const uint8_t*>(id.data()),id.length());
            send(resp);
        } else {
            serverRef.removeSession(shared_from_this());
        }
    }
    else if (authenticated) {
        // Forward to application handler
        auto respond = [this, self=shared_from_this()](uint8_t rpId, uint8_t rpStatus, uint32_t rpSeq, const std::vector<uint8_t>& rpBody){
            uint32_t tot = static_cast<uint32_t>(10 + rpBody.size());
            std::vector<uint8_t> msg(10 + rpBody.size());
            uint32_t be = htonl(tot);
            std::memcpy(msg.data(), &be, 4);
            msg[4]=rpId; msg[5]=rpStatus;
            uint32_t seqbe = htonl(rpSeq);
            std::memcpy(msg.data()+6, &seqbe, 4);
            std::memcpy(msg.data()+10, rpBody.data(), rpBody.size());
            send(msg);
        };
        if (serverRef.onRequest_) {
            serverRef.onRequest_(headerBuf[4], headerBuf[5], ntohl(*reinterpret_cast<uint32_t*>(headerBuf.data()+6)), bodyBuf, respond);
        }
    }

    // Continue reading
    doReadHeader();
}

void TurboNetServer::Session::send(const std::vector<uint8_t>& message) {
    auto self = shared_from_this();
    boost::asio::async_write(socket,
        boost::asio::buffer(message),
        boost::asio::bind_executor(strand_, [self](const boost::system::error_code&, std::size_t){ }));
}

// ----- Server Implementation -----
TurboNetServer::TurboNetServer(uint16_t port,
                               std::size_t maxConn,
                               std::size_t ioThreads)
    : ioCtx_(),
      workGuard_(boost::asio::make_work_guard(ioCtx_)),
      acceptor_(ioCtx_, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      maxConnections_(maxConn)
{
    for (std::size_t i = 0; i < ioThreads; ++i)
        ioThreads_.emplace_back([this]{ ioCtx_.run(); });
}

TurboNetServer::~TurboNetServer() {
    stop();
    workGuard_.reset();
    ioCtx_.stop();
    for (auto& t : ioThreads_)
        if (t.joinable()) t.join();
}

void TurboNetServer::setAuthHandler(AuthHandler auth) {
    onAuth_ = std::move(auth);
}

void TurboNetServer::setMaxConnections(std::size_t maxConn) {
    maxConnections_ = maxConn;
}

void TurboNetServer::start(PacketHandler handler) {
    onRequest_ = std::move(handler);
    doAccept();
}

void TurboNetServer::stop() {
    boost::system::error_code ec;
    acceptor_.close(ec);
    std::lock_guard<std::mutex> lock(sessionsMutex_);
    for (auto& s : sessions_) {
        s->socket.close(ec);
    }
    sessions_.clear();
}

void TurboNetServer::doAccept() {
    auto socket = std::make_shared<boost::asio::ip::tcp::socket>(ioCtx_);
    acceptor_.async_accept(*socket, [this, socket](const boost::system::error_code& ec){
        if (!ec) {
            std::lock_guard<std::mutex> lock(sessionsMutex_);
            if (sessions_.size() < maxConnections_) {
                auto sess = std::make_shared<Session>(std::move(*socket), *this);
                sessions_.insert(sess);
                sess->start();
            }
        }
        doAccept();
    });
}

void TurboNetServer::removeSession(std::shared_ptr<Session> sess) {
    std::lock_guard<std::mutex> lock(sessionsMutex_);
    sessions_.erase(sess);
}

} // namespace turbonet
