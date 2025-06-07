#pragma once

#include <boost/asio.hpp>
#include <thread>
#include <functional>
#include <vector>
#include <memory>
#include <mutex>
#include <cstdint>
#include <unordered_set>

namespace turbonet {

using PacketHandler = std::function<void(uint8_t packetId,
                                          uint8_t status,
                                          uint32_t sequence,
                                          const std::vector<uint8_t>& payload,
                                          std::function<void(uint8_t,uint8_t,uint32_t,const std::vector<uint8_t>&)> respond)>;
using AuthHandler   = std::function<bool(const std::string& clientId)>;

class TurboNetServer {
public:
    // port: TCP port, maxConnections: pool size
    TurboNetServer(uint16_t port,
                   std::size_t maxConnections = 100,
                   std::size_t ioThreads = std::thread::hardware_concurrency());
    ~TurboNetServer();

    // Start accepting authenticated sessions
    void start(PacketHandler handler);
    // Stop server and close all connections
    void stop();

    // Set authentication callback; return true to allow bind
    void setAuthHandler(AuthHandler auth);
    // Set maximum concurrent connections
    void setMaxConnections(std::size_t maxConn);

private:
    struct Session : public std::enable_shared_from_this<Session> {
        Session(boost::asio::ip::tcp::socket sock,
                TurboNetServer& server);
        void start();
        void doReadHeader();
        void onReadHeader(const boost::system::error_code& ec, std::size_t);
        void doReadBody(uint32_t len);
        void onReadBody(const boost::system::error_code& ec, std::size_t);
        void send(const std::vector<uint8_t>& message);

        boost::asio::ip::tcp::socket socket;
        // boost::asio::strand<boost::asio::io_context::executor_type> strand_;
        boost::asio::strand<boost::asio::ip::tcp::socket::executor_type> strand_;
        std::array<uint8_t,10> headerBuf;
        std::vector<uint8_t>   bodyBuf;
        TurboNetServer&        serverRef;
        bool                   authenticated{false};
    };

    void doAccept();
    void removeSession(std::shared_ptr<Session> sess);

    boost::asio::io_context                                        ioCtx_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> workGuard_;
    boost::asio::ip::tcp::acceptor                                 acceptor_;
    std::vector<std::thread>                                ioThreads_;

    PacketHandler                                    onRequest_;
    AuthHandler                                      onAuth_;
    std::mutex                                       sessionsMutex_;
    std::unordered_set<std::shared_ptr<Session>>     sessions_;
    std::size_t                                      maxConnections_;
};

} // namespace turbonet
