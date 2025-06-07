#include "turbonet_server.h"
#include "turbonet_client.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>

using namespace turbonet;

int main() {
    // 1. Listen for upstream producers on port 8000
    TurboNetServer server(8000, /*maxConns=*/50);

    // 2. Connect downstream to processor at localhost:9000
    auto client = std::make_shared<TurboNetClient>(5000, 5000, 10000);
    client->connect("127.0.0.1", 9000, 3000,
        [&](const boost::system::error_code& ec){
            if (ec) std::cerr << "Downstream connect failed: " << ec.message() << "\n";
            else    std::cout << "Connected downstream\n";
        });

    // Map originalSeq -> (downstreamSeq, respond callback)
    // Protected by mutex since callbacks run on different strands/threads
    struct Pending { uint32_t downstreamSeq;
                     std::function<void(uint8_t,uint8_t,uint32_t,const std::vector<uint8_t>&)> respond; };
    std::mutex                    mapMtx;
    std::unordered_map<uint32_t, Pending> pendingMap;

    // When downstream replies, look up and invoke only that session’s respond
    client->setPacketHandler(
        [&](uint8_t pid, uint8_t status, uint32_t dseq, const std::vector<uint8_t>& body){
            std::function<void(uint8_t,uint8_t,uint32_t,const std::vector<uint8_t>&)> respond;
            uint32_t origSeq = 0;
            {
                std::lock_guard<std::mutex> lk(mapMtx);
                for (auto it = pendingMap.begin(); it != pendingMap.end(); ++it) {
                    if (it->second.downstreamSeq == dseq) {
                        origSeq = it->first;
                        respond = std::move(it->second.respond);
                        pendingMap.erase(it);
                        break;
                    }
                }
            }
            if (respond) {
                // forward reply back to that producer session
                respond(pid, status, origSeq, body);
            } else {
                std::cerr << "[gateway] Unexpected downstream seq=" << dseq << "\n";
            }
        });

    // 3. Start accepting producer connections
    server.setAuthHandler([](auto const&){ return true; });
    server.start(
        [&](uint8_t pid,
            uint8_t status,
            uint32_t seq,
            const std::vector<uint8_t>& body,
            auto respond)
    {
        // Only forward request packets (0x02)
        if (pid == 0x02) {
            // sendRequest returns new downstream sequence
            uint32_t dseq = client->sendRequest(body.data(), body.size());
            {
                std::lock_guard<std::mutex> lk(mapMtx);
                // stash downstream seq + respond callback
                pendingMap.emplace(seq, Pending{dseq, respond});
            }
        }
        else {
            // direct echo for other packets
            respond(pid, status, seq, body);
        }
    });

    std::cout << "Gateway running. Press Enter to stop...\n";
    std::cin.get();
    server.stop();
    client->close();
    return 0;
}
