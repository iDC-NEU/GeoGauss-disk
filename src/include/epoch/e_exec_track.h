#ifndef OPENGAUSS_E_EXEC_TRACK_H
#define OPENGAUSS_E_EXEC_TRACK_H

/*
 * 统计某 epoch 内「正在执行 SQL、尚未完成读写集采集」的事务数。
 * +1：首次 OCC 访问（非 START gate，避免空闲连接误计）
 * -1：写集已 pack/change_set 入账，或只读 commit，或 abort
 */
void EpochMaybeMarkTxnExecuting(void);
void EpochReleaseTxnExecuting(void);

#endif /* OPENGAUSS_E_EXEC_TRACK_H */
