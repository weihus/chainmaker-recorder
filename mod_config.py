import re
import os
from glob import glob



PATHS = "build/config/*"
files = glob(PATHS)
node1_seed = ""
for path in files:
    yml = os.path.join(path, "chainmaker.yml")
    with open(yml, "r") as f1:#, open("%s.bak" % yml, "w") as f2:
        num = 0
        for line in f1:
            regex = "ip4/127.0.0.1"
            # seeds
            if (regex in line and "format" not in line) or "/dns/wx-org1-chainmaker-org-test-instance/" in line:
                num += 1
                if num == 1:
                    node1_seed = line.split("p2p/")[-1].split('"')[0]
                    print(node1_seed)
                    break
                # line = line.replace(regex, "dns/wx-org"+str(num)+"-chainmaker-org-test-instance")
            # mysql config
            # elif "chainmaker_recorder" in line:
                # line = line.replace(regex, "root:123456"+str(num)+"chainmaker:chainmaker")
                # line = line.replace(regex, "127.0.0.1"+str(num)+"storage-mysql")
            # f2.write(line)
    # os.remove(yml)
    # os.rename("%s.bak" % yml, yml)


svc = os.path.join("module/net/net_service.go")
with open(svc, "r") as f1, open("%s.bak" % svc, "w") as f2:
    num = 0
    for line in f1:
        if "msgType == 1" in line and "node" in line:
            line = re.sub(re.findall('(?=)[\w]+(?=")', line)[0], node1_seed, line)
        f2.write(line)
    os.remove(svc)
    os.rename("%s.bak" % svc, svc)

libp2p = os.path.join("submodule/net-libp2p@v1.2.0/libp2pnet/libp2p_net.go")
print("node1_seed:", node1_seed)
with open(libp2p, "r") as f1, open("%s.bak" % libp2p, "w") as f2:
    num = 0
    for line in f1:
        if "node !=" in line:
            line = re.sub(re.findall('(?=)[\w]+(?=")', line)[0], node1_seed, line)
            # print(line)
        f2.write(line)
    os.remove(libp2p)
    os.rename("%s.bak" % libp2p, libp2p)


# node1 = os.path.join("build/config/node1/chainmaker.yml")
# with open(node1, "r") as f1, open("%s.bak" % node1, "w") as f2:
#     num = 0
#     for line in f1:
#         if '- "/ip4/127.0.0.1/tcp/' in line:
#             # line = "    " + line.strip()
#             # before = line.split("-")[0]
#             after = line.split("-")[-1]
#             line = "    -" + after
#             # print(line)
#         # print(line)
#         f2.write(line)
#     os.remove(node1)
#     os.rename("%s.bak" % node1, node1)
#
# node2 = os.path.join("build/config/node2/chainmaker.yml")
# with open(node2, "r") as f1, open("%s.bak" % node2, "w") as f2:
#     num = 0
#     for line in f1:
#         if '- "/ip4/127.0.0.1/tcp/' in line:
#             # line = "    " + line.strip()
#             after = line.split("-")[-1]
#             line = "    -" + after
#             # print(line)
#         # print(line)
#         f2.write(line)
#     os.remove(node2)
#     os.rename("%s.bak" % node2, node2)
#
# node3 = os.path.join("build/config/node3/chainmaker.yml")
# with open(node3, "r") as f1, open("%s.bak" % node3, "w") as f2:
#     num = 0
#     for line in f1:
#         if '- "/ip4/127.0.0.1/tcp/' in line:
#             # line = "    " + line.strip()
#             after = line.split("-")[-1]
#             line = "    -" + after
#             # print(line)
#         # print(line)
#         f2.write(line)
#     os.remove(node3)
#     os.rename("%s.bak" % node3, node3)
#
# node4 = os.path.join("build/config/node4/chainmaker.yml")
# with open(node4, "r") as f1, open("%s.bak" % node4, "w") as f2:
#     num = 0
#     for line in f1:
#         if '- "/ip4/127.0.0.1/tcp/' in line:
#             # line = "    " + line.strip()
#             after = line.split("-")[-1]
#             line = "    -" + after
#             # print(line)
#         # print(line)
#         f2.write(line)
#     os.remove(node4)
#     os.rename("%s.bak" % node4, node4)