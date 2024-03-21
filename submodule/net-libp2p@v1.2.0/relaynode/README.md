# 使用
```shell
 go build
 ./main
```
e.g: 有四个node：A、B、C、D

```shell
A: QmQSkmXoU1kBh5q5obnp4JtpDSGCVnxQ4YEdAsghzpqo5n
B: QmYQEG7nhA6LYADzb14jssu8KjdFQFaE1XxWNPg96eMv47
C: QmPeMgEHTg9hgGsZqNzjiDnnJhKvx8o5MfYcGBeeBZCEh5
D: QmUQAfvMtVT6JGEgQaGCNTVDpqUo8ckhsAYyfh1xoQJjKd
relay: /ip4/x.x.x.x/tcp/11305/p2p/Qmcg2zay8QDykQnoePf8aytsRqiEFKwbS91Uj2zcRnqUwv
```

1. seed 配置

   A、B、C、D seed可进行如下配置

   规则: `relay地址/p2p-circuit/p2p/目标nodeId`

   ```yaml
   seeds:
       - "/ip4/x.x.x.x/tcp/11305/p2p/Qmcg2zay8QDykQnoePf8aytsRqiEFKwbS91Uj2zcRnqUwv/p2p-circuit/p2p/QmQSkmXoU1kBh5q5obnp4JtpDSGCVnxQ4YEdAsghzpqo5n"
       - "/ip4/x.x.x.x/tcp/11305/p2p/Qmcg2zay8QDykQnoePf8aytsRqiEFKwbS91Uj2zcRnqUwv/p2p-circuit/p2p/QmYQEG7nhA6LYADzb14jssu8KjdFQFaE1XxWNPg96eMv47"
       - "/ip4/x.x.x.x/tcp/11305/p2p/Qmcg2zay8QDykQnoePf8aytsRqiEFKwbS91Uj2zcRnqUwv/p2p-circuit/p2p/QmPeMgEHTg9hgGsZqNzjiDnnJhKvx8o5MfYcGBeeBZCEh5"
       - "/ip4/x.x.x.x/tcp/11305/p2p/Qmcg2zay8QDykQnoePf8aytsRqiEFKwbS91Uj2zcRnqUwv/p2p-circuit/p2p/QmUQAfvMtVT6JGEgQaGCNTVDpqUo8ckhsAYyfh1xoQJjKd"
   ```

   relay可不配置seed，等待其他peer与自己建立连接。 
 
   如果一个地址是中继地址，会先与中继建立连接，然后通过中继与目标节点建立连接

3. trust root配置

      A、B、C、D、realy互相将彼此的ca配置到trust_root列表中

将中继的`max_peer_count_allow`参数稍微调高一些，默认是20，即只能与20个node建立连接。
