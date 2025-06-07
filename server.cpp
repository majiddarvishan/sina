#include "turbonet_server.h"
#include <iostream>
#include <unordered_set>

int main()
{
    using namespace turbonet;
    // Authorized client IDs
    std::unordered_set<std::string> validClients = {"client_123", "client_abc"};

    // Create server on port 9000, max 10 connections
    TurboNetServer server(9000, 10);

    // Set authentication: only allow IDs in validClients set
    server.setAuthHandler([&](const std::string& clientId) {
        bool ok = validClients.count(clientId) > 0;
        std::cout << "Auth attempt: " << clientId << " -> " << (ok?"accepted":"rejected") << "";
        return ok;
    });

    // Handle requests from authenticated clients
    server.start([&](uint8_t packetId,
                     uint8_t status,
                     uint32_t seq,
                     const std::vector<uint8_t>& payload,
                     std::function<void(uint8_t,uint8_t,uint32_t,const std::vector<uint8_t>&)> respond) {
        std::string msg(payload.begin(), payload.end());
        std::cout << "Received packetId=0x" << std::hex << int(packetId)
                  << " seq=" << std::dec << seq
                  << " payload='" << msg << "'";

        // Echo back with response packetId=0x82
        std::string reply = "Echo: " + msg;
        respond(0x82, 0x00, seq, std::vector<uint8_t>(reply.begin(), reply.end()));
    });

    std::cout << "Server running on port 9000. Press Enter to stop.";
    std::cin.get();
    server.stop();
    return 0;
}
