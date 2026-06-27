#ifndef OPENGAUSS_E_NET_H
#define OPENGAUSS_E_NET_H

#include <cstdint>
#include <string>

bool EpochNetInit(void);
void EpochNetShutdown(void);
bool EpochNetPublish(const std::string& payload);
bool EpochNetPublishDirect(const std::string& payload);
bool EpochNetPublishTo(uint32_t target_node, const std::string& payload);
bool EpochNetTryReceive(std::string* payload, int* from_node);

bool EpochPeerIsConnected(uint32_t nodeId);
void EpochMarkPeerConnected(uint32_t nodeId);
bool EpochAllPeersConnected(void);

/* 立即发送 hello（对端 RECV 时回包，完成双向握手） */
void EpochNetPublishHello(void);
/* ZMQ slow-joiner：对端晚启动时周期重发 hello，直至 peer 连通 */
void EpochNetMaybePublishHello(void);

#endif /* OPENGAUSS_E_NET_H */
