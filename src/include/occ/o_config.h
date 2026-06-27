#ifndef OPENGAUSS_O_CONFIG_H
#define OPENGAUSS_O_CONFIG_H

/*
 * 磁盘表并发控制模式开关：
 * - enable_occ_disk=on, enable_epoch_multimaster=off  -> 单机 OCC
 * - enable_epoch_multimaster=on                     -> epoch 多主（自动启用执行器钩子）
 * - 两者均为 off                                    -> 原生 openGauss heap 路径
 */
extern bool g_enable_occ_disk;

extern void OccConfigRegisterGucs(void);

static inline bool OccDiskAccessHooksEnabled(void)
{
    extern bool g_enable_epoch_multimaster;
    if (g_enable_epoch_multimaster) {
        return true;
    }
    return g_enable_occ_disk;
}

static inline bool OccSingleNodeModeEnabled(void)
{
    extern bool g_enable_epoch_multimaster;
    return g_enable_occ_disk && !g_enable_epoch_multimaster;
}

static inline bool OccEpochMultiMasterEnabled(void)
{
    extern bool g_enable_epoch_multimaster;
    return g_enable_epoch_multimaster;
}

#endif /* OPENGAUSS_O_CONFIG_H */
