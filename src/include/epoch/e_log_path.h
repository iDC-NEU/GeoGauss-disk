#ifndef OPENGAUSS_E_LOG_PATH_H
#define OPENGAUSS_E_LOG_PATH_H

#include <cstddef>

/* 生成 {DataDir}/{basename}_{YYYYMMDD_HHMMSS}.log */
bool EpochFormatLogPath(char* path, size_t path_size, const char* basename);

#endif /* OPENGAUSS_E_LOG_PATH_H */
