#include "turbonet_client.h"

#include <iostream>
#include <cstring>

int main()
{
    boost::asio::io_context io_context;
    // Create client with 5s read/write timeout and 10s response timeout
    auto client = std::make_shared<turbonet::TurboNetClient>(&io_context,
        "test_client",
        "127.0.0.1",
        9000,
        100,
        [](const std::string& serverId) -> void {
            std::cout << "bind successfully to server='" << serverId << "'\n";
        },
        [](const std::string& serverId) -> void {
            std::cout << "error to server='" << serverId << "'\n";
        },
        5000, 5000, 10000);
    // client->setClientId("client_123");
    // client->setBindHandler([](const std::string& serverId) -> void {
    //     std::cout << "bind successfully to server='" << serverId << "'\n";
    // });

    // Set packet handler
    client->setPacketHandler([](uint8_t packetId, uint8_t status, uint32_t seq, const std::vector<uint8_t>& payload) {
        std::string msg(payload.begin(), payload.end());
        std::cout << "Received packetId=" << std::hex << int(packetId)
                  << " status=" << std::hex << int(status)
                  << " seq=" << seq << " payload='" << msg << "'\n";
    });

    // Set timeout handler
    client->setTimeoutHandler([](uint32_t seq) {
        std::cerr << "Request timed out, seq=" << seq << "\n";
    });

    client->start();

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(5s);

    // Send a request and get sequence
    const char* req = "hello_server";
    uint32_t seq = client->sendRequest(reinterpret_cast<const uint8_t*>(req), std::strlen(req));
    std::cout << "Sent request seq=" << seq << "\n";

    // Run until enter pressed
    std::cout << "Press Enter to exit...\n";
    std::cin.get();
    client->close();
    return 0;
}
