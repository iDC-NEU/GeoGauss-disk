#ifndef OPENGAUSS_E_NET_H
#define OPENGAUSS_E_NET_H

#include <cstdint>
#include <string>

bool EpochNetInit(void);
/* Pack/Send：勿重复 Init；等 epochcoord BootstrapOwner 完成绑定 */
bool EpochNetIsReady(void);
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

/* DBR 看复制连接上 TCP 还没交给网卡的字节。高于此值视为上行堵住。 */
#define EPOCH_UPLINK_HIGH_BYTES (16u * 1024u)
#define EPOCH_UPLINK_LOW_BYTES (8u * 1024u)

/* Send 线程刷新。堵住时 DeferPayload 为真，不要再把数据写集送进 ZMQ。 */
void EpochNetPollUplink(void);
bool EpochNetDeferPayload(void);
/* 内核发送缓冲持续偏高（tc 限速），可以做 DBR 后移。 */
bool EpochNetUplinkCongested(void);
/* 已经拷进 ZMQ 的数据字节，用来在内核计数追上之前先停手。 */
void EpochNetNotePayloadSent(size_t nbytes);

#endif /* OPENGAUSS_E_NET_H */
