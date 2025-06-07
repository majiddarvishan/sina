#include "turbonet_client.h"

#include <boost/asio/write.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/connect.hpp>

#include <cstring>

namespace turbonet {

// Helpers
uint32_t TurboNetClient::toBigEndian(uint32_t v) {
    return htonl(v);
}
uint32_t TurboNetClient::fromBigEndian(const uint8_t* b) {
    uint32_t v;
    std::memcpy(&v, b, 4);
    return ntohl(v);
}

// Constructor / Destructor
TurboNetClient::TurboNetClient(int readTimeoutMs,
                               int writeTimeoutMs,
                               int responseTimeoutMs,
                               std::size_t ioThreads)
    : ioCtx_(),
      workGuard_(boost::asio::make_work_guard(ioCtx_)),
      socket_(ioCtx_),
      connectTimer_(ioCtx_),
      readTimer_(ioCtx_),
      writeTimer_(ioCtx_),
      responseSweepTimer_(ioCtx_),
      strand_(boost::asio::make_strand(ioCtx_)),
      readTimeoutMs_(readTimeoutMs),
      writeTimeoutMs_(writeTimeoutMs),
      responseTimeoutMs_(responseTimeoutMs) {
    running_ = true;
    for (std::size_t i = 0; i < ioThreads; ++i) {
        ioThreads_.emplace_back([this] { ioCtx_.run(); });
    }
}

TurboNetClient::~TurboNetClient() {
    close();
    workGuard_.reset();
    ioCtx_.stop();
    for (auto& t : ioThreads_) if (t.joinable()) t.join();
}

// Public API
void TurboNetClient::setClientId(const std::string& clientId) {
    clientId_ = clientId;
}
void TurboNetClient::setBindHandler(BindHandler handler) {
    onBind_ = std::move(handler);
}
void TurboNetClient::setPacketHandler(PacketHandler handler) {
    onPacket_ = std::move(handler);
}
void TurboNetClient::setTimeoutHandler(TimeoutHandler handler) {
    onTimeout_ = std::move(handler);
}

void TurboNetClient::setCloseHandler(CloseHandler handler) {
    onClose_ = std::move(handler);
}

void TurboNetClient::connect(const std::string& host,
                             uint16_t port,
                             int timeoutMs,
                             std::function<void(const boost::system::error_code&)> onConnect) {
    connectHandler_ = std::move(onConnect);
    boost::asio::ip::tcp::resolver resolver(ioCtx_);
    auto endpoints = resolver.resolve(host, std::to_string(port));

    startConnectTimer(timeoutMs);
    boost::asio::async_connect(socket_, endpoints,
        boost::asio::bind_executor(strand_, [self = shared_from_this()](const boost::system::error_code& ec, auto&) {
            self->cancelConnectTimer();
            if (!ec) {
                // Auto-bind
                if (!self->clientId_.empty()) {
                    uint32_t seq = self->sequenceCounter_++;
                    self->sendPacket(0x01, 0x00, seq,
                                     reinterpret_cast<const uint8_t*>(self->clientId_.data()),
                                     self->clientId_.size());
                }
                self->doReadHeader();
            }
            if (self->connectHandler_) self->connectHandler_(ec);
        }));
}

void TurboNetClient::sendPacket(uint8_t packetId,
                                uint8_t status,
                                uint32_t sequence,
                                const uint8_t* data,
                                std::size_t len) {
    std::vector<uint8_t> pkt(10 + len);
    uint32_t totalLen = 10 + static_cast<uint32_t>(len);
    uint32_t beLen = toBigEndian(totalLen);
    std::memcpy(pkt.data(), &beLen, 4);
    pkt[4] = packetId;
    pkt[5] = status;
    uint32_t beSeq = toBigEndian(sequence);
    std::memcpy(pkt.data() + 6, &beSeq, 4);
    std::memcpy(pkt.data() + 10, data, len);

    boost::asio::post(strand_, [self = shared_from_this(), pkt = std::move(pkt)]() mutable {
        bool writing = !self->txBuffer_.empty();
        self->txBuffer_.insert(self->txBuffer_.end(), pkt.begin(), pkt.end());
        if (!writing) self->doWrite();
    });
}

uint32_t TurboNetClient::sendRequest(const uint8_t* data, std::size_t len) {
    uint32_t seq = sequenceCounter_++;
    sendPacket(0x02, 0x00, seq, data, len);
    startResponseTimer(seq);
    return seq;
}

void TurboNetClient::close() {
    running_ = false;
    boost::system::error_code ec;
    socket_.close(ec);
    cancelConnectTimer();
    cancelReadTimer();
    cancelWriteTimer();
    responseSweepTimer_.cancel(ec);

    // Notify user of closure
    if (onClose_) {
        onClose_();
    }
}

// I/O Implementation
void TurboNetClient::doReadHeader() {
    startReadTimer();
    boost::asio::async_read(socket_, boost::asio::buffer(headerBuf_),
        boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec, auto n) {
            self->onReadHeader(ec, n);
        }));
}

void TurboNetClient::onReadHeader(const boost::system::error_code& ec, std::size_t) {
    if (ec) { cancelReadTimer(); return; }

    uint32_t packetLen = fromBigEndian(headerBuf_.data());
    lastPacketId_ = headerBuf_[4];
    lastStatus_   = headerBuf_[5];
    lastSequence_ = fromBigEndian(headerBuf_.data() + 6);

    if (packetLen >= 10) doReadBody(packetLen - 10);
    else doReadHeader();
}

void TurboNetClient::doReadBody(uint32_t bodyLen) {
    bodyBuf_.resize(bodyLen);
    boost::asio::async_read(socket_, boost::asio::buffer(bodyBuf_),
        boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec, auto n) {
            self->onReadBody(ec, n);
        }));
}

void TurboNetClient::onReadBody(const boost::system::error_code& ec, std::size_t) {
    cancelReadTimer();
    if (!ec) {
        // Bind-response
        if (lastPacketId_ == 0x81 && onBind_) {
            std::string sid(bodyBuf_.begin(), bodyBuf_.end());
            onBind_(sid);
        }
        // Generic
        if (onPacket_) {
            onPacket_(lastPacketId_, lastStatus_, lastSequence_, bodyBuf_);
        }
    }
    if (running_) doReadHeader();
}

void TurboNetClient::doWrite() {
    startWriteTimer();
    boost::asio::async_write(socket_, boost::asio::buffer(txBuffer_),
        boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec, auto n) {
            self->onWrite(ec, n);
        }));
}

void TurboNetClient::onWrite(const boost::system::error_code& ec, std::size_t) {
    cancelWriteTimer();
    if (!ec) txBuffer_.clear();
}

// Timer Implementation
void TurboNetClient::startConnectTimer(int timeoutMs) {
    if (timeoutMs <= 0) return;
    connectTimer_.expires_after(std::chrono::milliseconds(timeoutMs));
    connectTimer_.async_wait(boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec) {
        self->onConnectTimeout(ec);
    }));
}
void TurboNetClient::cancelConnectTimer() {
    boost::system::error_code ec; connectTimer_.cancel(ec);
}
void TurboNetClient::onConnectTimeout(const boost::system::error_code& ec) {
    if (!ec) socket_.close();
}

void TurboNetClient::startReadTimer() {
    if (readTimeoutMs_ <= 0) return;
    readTimer_.expires_after(std::chrono::milliseconds(readTimeoutMs_));
    readTimer_.async_wait(boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec) {
        self->onReadTimeout(ec);
    }));
}
void TurboNetClient::cancelReadTimer() {
    boost::system::error_code ec; readTimer_.cancel(ec);
}
void TurboNetClient::onReadTimeout(const boost::system::error_code& ec) {
    if (!ec) socket_.close();
}

void TurboNetClient::startWriteTimer() {
    if (writeTimeoutMs_ <= 0) return;
    writeTimer_.expires_after(std::chrono::milliseconds(writeTimeoutMs_));
    writeTimer_.async_wait(boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec) {
        self->onWriteTimeout(ec);
    }));
}
void TurboNetClient::cancelWriteTimer() {
    boost::system::error_code ec; writeTimer_.cancel(ec);
}
void TurboNetClient::onWriteTimeout(const boost::system::error_code& ec) {
    if (!ec) socket_.close();
}

// Response Timeout
void TurboNetClient::startResponseTimer(uint32_t sequence) {
    if (responseTimeoutMs_ <= 0) return;
    auto expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(responseTimeoutMs_);
    {
        std::lock_guard lock(responseMutex_);
        responseMap_[sequence] = expiry;
        responseQueue_.push({sequence, expiry});
    }
    responseSweepTimer_.expires_after(std::chrono::milliseconds(responseTimeoutMs_));
    responseSweepTimer_.async_wait(boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec) {
        self->checkAndFireResponseTimers(ec);
    }));
}

void TurboNetClient::cancelResponseTimer(uint32_t sequence) {
    std::lock_guard lock(responseMutex_);
    responseMap_.erase(sequence);
}

void TurboNetClient::checkAndFireResponseTimers(const boost::system::error_code& ec) {
    if (ec) return;
    auto now = std::chrono::steady_clock::now();
    std::vector<uint32_t> expired;
    {
        std::lock_guard lock(responseMutex_);
        while (!responseQueue_.empty() && responseQueue_.top().expiry <= now) {
            uint32_t seq = responseQueue_.top().sequence;
            responseQueue_.pop();
            if (responseMap_.erase(seq)) expired.push_back(seq);
        }
    }
    for (auto seq : expired) {
        if (onTimeout_) onTimeout_(seq);
    }
    // Reschedule next
    std::chrono::steady_clock::time_point next = now + std::chrono::hours(24);
    {
        std::lock_guard lock(responseMutex_);
        if (!responseQueue_.empty()) next = responseQueue_.top().expiry;
    }
    responseSweepTimer_.expires_at(next);
    responseSweepTimer_.async_wait(boost::asio::bind_executor(strand_, [self = shared_from_this()](auto ec) {
        self->checkAndFireResponseTimers(ec);
    }));
}

} // namespace turbonet
