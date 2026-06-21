-- epoch 冒烟测试表结构（各节点本地执行，与 TPCC runDatabaseBuild 类似）
DROP TABLE IF EXISTS epoch_smoke;
CREATE TABLE epoch_smoke (
    id          INT PRIMARY KEY,
    node_id     INT NOT NULL,
    cnt         BIGINT NOT NULL DEFAULT 0
);
