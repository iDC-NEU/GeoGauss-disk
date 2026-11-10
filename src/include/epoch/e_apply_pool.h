#ifndef OPENGAUSS_E_APPLY_POOL_H
#define OPENGAUSS_E_APPLY_POOL_H

/*
 * apply 池：只剩生命周期管理。队列与游标都在 e_apply_shmem。
 */

extern void EpochApplyPoolInit(void);
extern void EpochApplyPoolShutdown(void);

#endif /* OPENGAUSS_E_APPLY_POOL_H */
