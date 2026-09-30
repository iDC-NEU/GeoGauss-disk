#ifndef OPENGAUSS_E_LOG_PATH_H
#define OPENGAUSS_E_LOG_PATH_H

#include <cstddef>

/* epoch worker 主线程 init 后调用，供 physical/recv 等 std::thread 写日志 */
void EpochLogPathInit(void);

/* pipeline 内部 std::thread 入口调用；name 须 <16 字符（含 '\0'） */
void EpochSetAuxThreadName(const char* name);

/* 生成 {DataDir}/{basename}_{YYYYMMDD_HHMMSS}.log */
bool EpochFormatLogPath(char* path, size_t path_size, const char* basename);

#endif /* OPENGAUSS_E_LOG_PATH_H */
