#ifndef OPENGAUSS_E_NET_H
#define OPENGAUSS_E_NET_H

#include <cstdint>
#include <string>

bool EpochNetInit(void);
void EpochNetShutdown(void);
bool EpochNetPublish(const std::string& payload);
bool EpochNetTryReceive(std::string* payload);

bool EpochPeerIsConnected(uint32_t nodeId);
void EpochMarkPeerConnected(uint32_t nodeId);
bool EpochAllPeersConnected(void);

/* ZMQ slow-joiner：对端晚启动时周期重发 hello，直至 peer 连通 */
void EpochNetMaybePublishHello(void);

#endif /* OPENGAUSS_E_NET_H */
