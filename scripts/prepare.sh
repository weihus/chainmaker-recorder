#!/usr/bin/env bash
#
# Copyright (C) BABEC. All rights reserved.
# Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

# check mac gun-getopt
function checkEnv() {
  if [ "$(uname)" == "Darwin" ];then
    getopt --test
    if [ "$?" != "4" ];then
      brew -v > /dev/null
      if [ "$?" != "0" ];then
        echo 'Please install brew for Mac: ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"'
      fi
      echo 'Please install gnu-getopt for Mac: brew install gnu-getopt and set to PATH (brew link --force gnu-getopt)'
      exit
    fi
  fi
}
checkEnv

set -e

VERSION='"2030100"'

NODE_CNT=$1
CHAIN_CNT=$2
P2P_PORT=$3
RPC_PORT=$4
VM_GO_RUNTIME_PORT=$5
VM_GO_ENGINE_PORT=$6

CURRENT_PATH=$(pwd)
PROJECT_PATH=$(dirname "${CURRENT_PATH}")
BUILD_PATH=${PROJECT_PATH}/build
CONFIG_TPL_PATH=${PROJECT_PATH}/config/config_tpl
BUILD_CRYPTO_CONFIG_PATH=${BUILD_PATH}/crypto-config
BUILD_CONFIG_PATH=${BUILD_PATH}/config
CRYPTOGEN_TOOL_PATH=${PROJECT_PATH}/tools/chainmaker-cryptogen
CRYPTOGEN_TOOL_BIN=${CRYPTOGEN_TOOL_PATH}/bin/chainmaker-cryptogen
CRYPTOGEN_TOOL_CONF=${CRYPTOGEN_TOOL_PATH}/config/crypto_config_template.yml
CRYPTOGEN_TOOL_PKCS11_KEYS=${CRYPTOGEN_TOOL_PATH}/config/pkcs11_keys.yml

BC_YML_TRUST_ROOT_LINE=$(awk '/trust roots list start/{print NR}' ${CONFIG_TPL_PATH}/chainconfig/bc_4_7.tpl)
BC_YML_TRUST_ROOT_LINE_END=$(awk '/trust roots list end/{print NR}' ${CONFIG_TPL_PATH}/chainconfig/bc_4_7.tpl)

function show_help() {
    echo "Usage:  "
    echo "    prepare.sh node_cnt(1/4/7/10/13/16) chain_cnt(1-4)"
    echo "               p2p_port(default:11301) rpc_port(default:12301)"
    echo "               vm_go_runtime_port(default:32351) vm_go_engine_port(default:22351)"
    echo "               -c consense-type: 1-TBFT,3-MAXBFT,4-RAFT "
    echo "               -l log-level: DEBUG,INFO,WARN,ERROR"
    echo "               -v docker-vm-enable: true,false"
    echo "                  --vtp  vm go transport protocol: tcp,uds"
    echo "                  --vlog vm go log level: DEBUG,INFO,WARN,ERROR"
    echo "               -h show help"
    echo "    eg1: prepare.sh 4 1"
    echo "    eg2: prepare.sh 4 1 11301 12301"
    echo "    eg2: prepare.sh 4 1 11301 12301 32351 22351"
    echo "    eg2: prepare.sh 4 1 11301 12301 32351 22351 -c 1 -l INFO -v true  --vtp=tcp --vlog=INFO"
    echo "    eg2: prepare.sh 4 1 11301 12301 32351 22351 -c 1 -l INFO -v false --vtp=tcp --vlog=INFO"
}

if ( [ $# -eq 1 ] && [ "$1" ==  "-h" ] ) ; then
    show_help
    exit 1
fi

if [ $# -eq 1 ]; then
    echo "invalid params"
    show_help
    exit 1
fi

function xsed() {
    system=$(uname)

    if [ "${system}" = "Linux" ]; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}


function check_params() {
    echo "begin check params..."
    if  [[ ! -n $NODE_CNT ]] ;then
        echo "node cnt is empty"
        show_help
        exit 1
    fi

    if  [ ! $NODE_CNT -eq 1 ] && [ ! $NODE_CNT -eq 4 ] && [ ! $NODE_CNT -eq 7 ]&& [ ! $NODE_CNT -eq 10 ]&& [ ! $NODE_CNT -eq 13 ]&& [ ! $NODE_CNT -eq 16 ];then
        echo "node cnt should be 1 or 4 or 7 or 10 or 13"
        show_help
        exit 1
    fi

    if  [[ ! -n $CHAIN_CNT ]] ;then
        echo "chain cnt is empty"
        show_help
        exit 1
    fi

    if  [ ${CHAIN_CNT} -lt 1 ] || [ ${CHAIN_CNT} -gt 4 ] ;then
        echo "chain cnt should be 1 - 4"
        show_help
        exit 1
    fi

    # 判断是否是数字
    if [ "$P2P_PORT" -gt 0 ] 2>/dev/null ;then
      # 判断数字范围
      if  [ ${P2P_PORT} -ge 60000 ] || [ ${P2P_PORT} -le 10000 ];then
        P2P_PORT=11301
      fi
    else
        P2P_PORT=11301
    fi
    echo "param P2P_PORT $P2P_PORT"

    if [ "$RPC_PORT" -gt 0 ] 2>/dev/null ;then
      if  [ ${RPC_PORT} -ge 60000 ] || [ ${RPC_PORT} -le 10000 ];then
        RPC_PORT=12301
      fi
    else
        RPC_PORT=12301
    fi
    echo "param RPC_PORT $RPC_PORT"

    if [ "$VM_GO_RUNTIME_PORT" -gt 0 ] 2>/dev/null ;then
      if  [ ${VM_GO_RUNTIME_PORT} -ge 60000 ] || [ ${VM_GO_RUNTIME_PORT} -le 10000 ];then
        VM_GO_RUNTIME_PORT=32351
      fi
    else
        VM_GO_RUNTIME_PORT=32351
    fi
    echo "param VM_GO_RUNTIME_PORT $VM_GO_RUNTIME_PORT"

    if [ "$VM_GO_ENGINE_PORT" -gt 0 ] 2>/dev/null ;then
      if  [ ${VM_GO_ENGINE_PORT} -ge 60000 ] || [ ${VM_GO_ENGINE_PORT} -le 10000 ];then
        VM_GO_ENGINE_PORT=22351
      fi
    else
        VM_GO_ENGINE_PORT=22351
    fi
    echo "param VM_GO_ENGINE_PORT $VM_GO_ENGINE_PORT"
}

function generate_certs() {
    #echo "begin generate certs, cnt: ${NODE_CNT}"
    mkdir -p ${BUILD_PATH}
    cd "${BUILD_PATH}"
    if [ -d crypto-config ]; then
        mkdir -p backup/backup_certs
        mv crypto-config  backup/backup_certs/crypto-config_$(date "+%Y%m%d%H%M%S")
    fi

    cp $CRYPTOGEN_TOOL_CONF crypto_config.yml
    cp $CRYPTOGEN_TOOL_PKCS11_KEYS pkcs11_keys.yml

    xsed "s%count: 4%count: ${NODE_CNT}%g" crypto_config.yml

    ${CRYPTOGEN_TOOL_BIN} generate -c ./crypto_config.yml  -p ./pkcs11_keys.yml
}

function generate_config() {
    LOG_LEVEL="" # default INFO
    CONSENSUS_TYPE=-1 # default  1
    MONITOR_PORT=14321
    PPROF_PORT=24321
    TRUSTED_PORT=13301
    VM_GO_CONTAINER_NAME_PREFIX="chainmaker-vm-go-container"
    ENABLE_VM_GO="" # default false
    VM_GO_LOG_LEVEL="" # default INFO
    VM_GO_TRANSPORT_PROTOCOL="" # default tcp, uds

    set -- $(getopt -u -o c:l:v: -l vtp:,vlog: "$@")   # -o 接收短参数， -l 接收长参数， 需要参数值的在参数后面添加:
    while [ -n "$1" ]; do
        case "$1" in
            -c) CONSENSUS_TYPE=$2
                 shift ;;
            -l) LOG_LEVEL=$2
                shift ;;
            -v) ENABLE_VM_GO=$2
                shift ;;
            --vtp)
                VM_GO_TRANSPORT_PROTOCOL=$2
                shift ;;
            --vlog)
                VM_GO_LOG_LEVEL=$2
                shift
        esac
        shift
    done

    # set CONSENSUS_TYPE
    if [ $CONSENSUS_TYPE == -1 ] ;then
      if  [ $NODE_CNT -gt 1 ] ;then
        read -p "input consensus type (1-TBFT(default),3-MAXBFT,4-RAFT): " tmp
        if  [ ! -z "$tmp" ] ;then
          if [ $tmp -eq 1 ] || [ $tmp -eq 3 ] || [ $tmp -eq 4 ] ;then
            CONSENSUS_TYPE=$tmp
          else
            echo "invalid consensus type [" $tmp "], so use default"
          fi
        fi
      else
        read -p "input consensus type (0-SOLO,1-TBFT(default),3-MAXBFT,4-RAFT): " tmp
        if  [ ! -z "$tmp" ] ;then
          if  [ $tmp -eq 0 ] || [ $tmp -eq 1 ] || [ $tmp -eq 3 ] || [ $tmp -eq 4 ] ;then
            CONSENSUS_TYPE=$tmp
          else
            echo "unknown consensus type [" $tmp "], so use default"
          fi
        fi
      fi
    fi
    if [ $CONSENSUS_TYPE == -1 ] ;then
          CONSENSUS_TYPE=1
    fi
    if [ $CONSENSUS_TYPE == 3 ] && [ $NODE_CNT -lt 4 ] ;then
      echo  "the current version of maxbft does not support the deployment of less than four nodes"
      exit
    fi
    echo "param CONSENSUS_TYPE $CONSENSUS_TYPE"

    # set LOG_LEVEL
    if [ "$LOG_LEVEL" == "" ] ;then
      read -p "input log level (DEBUG|INFO(default)|WARN|ERROR): " tmp
      if  [ ! -z "$tmp" ] ;then
        if  [ $tmp == "DEBUG" ] || [ $tmp == "INFO" ] || [ $tmp == "WARN" ] || [ $tmp == "ERROR" ];then
            LOG_LEVEL=$tmp
        else
          echo "unknown log level [" $tmp "], so use default"
        fi
      fi
    fi
    if [ "$LOG_LEVEL" == "" ] ;then
        LOG_LEVEL="INFO"
    fi
    echo "param LOG_LEVEL $LOG_LEVEL"

    # set ENABLE_VM_GO
    if [ "$ENABLE_VM_GO" == "" ] ;then
      read -p "enable vm go (YES|NO(default))" enable_vm_go
      if  [ ! -z "$enable_vm_go" ]; then
        if  [ $enable_vm_go == "yes" ] || [ $enable_vm_go == "YES" ]; then
            ENABLE_VM_GO="true"

            if [ "$VM_GO_TRANSPORT_PROTOCOL" == "" ] ;then
              read -p "vm go transport protocol (uds|tcp(default))" transport_protocol
              if [ ! -z "$transport_protocol" ]; then
                  if [ $transport_protocol == "tcp" ] || [ $transport_protocol == "TCP" ]; then
                      VM_GO_TRANSPORT_PROTOCOL="tcp"
                  elif [ $transport_protocol == "uds" ] || [ $transport_protocol == "UDS" ]; then
                      VM_GO_TRANSPORT_PROTOCOL="uds"
                  else
                      echo "unknown input [" $transport_protocol "], so use default"
                  fi
              fi
            fi
            if [ "$VM_GO_TRANSPORT_PROTOCOL" == "" ] ;then
              VM_GO_TRANSPORT_PROTOCOL="tcp"
            fi

            if [ "$VM_GO_LOG_LEVEL" == "" ] ;then
                read -p "input vm go log level (DEBUG|INFO(default)|WARN|ERROR): " vm_go_log_level
                if  [ ! -z "$vm_go_log_level" ] ;then
                if  [ $vm_go_log_level == "DEBUG" ] || [ $vm_go_log_level == "INFO" ] || [ $vm_go_log_level == "WARN" ] || [ $vm_go_log_level == "ERROR" ];then
                    VM_GO_LOG_LEVEL=$vm_go_log_level
                else
                    echo "unknown vm go log level [" $vm_go_log_level "], so use default"
                fi
              fi
            fi
            if [ "$VM_GO_LOG_LEVEL" == "" ] ;then
              VM_GO_LOG_LEVEL="INFO"
            fi

        fi
      fi
    fi
    if [ "$ENABLE_VM_GO" == "" ] ;then
      ENABLE_VM_GO="false"
    elif [ $ENABLE_VM_GO == "true" ] ;then
      echo "param VM_GO_LOG_LEVEL $VM_GO_LOG_LEVEL"
      echo "param VM_GO_TRANSPORT_PROTOCOL $VM_GO_TRANSPORT_PROTOCOL"
    fi
    echo "param ENABLE_VM_GO $ENABLE_VM_GO"
    echo

    cd "${BUILD_PATH}"
    if [ -d config ]; then
        mkdir -p backup/backup_config
        mv config  backup/backup_config/config_$(date "+%Y%m%d%H%M%S")
    fi

    mkdir -p ${BUILD_PATH}/config
    cd ${BUILD_PATH}/config

    node_count=$(ls -l $BUILD_CRYPTO_CONFIG_PATH|grep "^d"| wc -l)
    echo "config node total $node_count"
    for ((i = 1; i < $node_count + 1; i = i + 1)); do
#    for ((i = 1; i < $NODE_CNT + 1; i = i + 1)); do
        echo "begin generate node$i config..."
        mkdir -p ${BUILD_PATH}/config/node$i
        mkdir -p ${BUILD_PATH}/config/node$i/chainconfig
        cp $CONFIG_TPL_PATH/log.tpl node$i/log.yml
        xsed "s%{log_level}%$LOG_LEVEL%g" node$i/log.yml
        cp $CONFIG_TPL_PATH/chainmaker.tpl node$i/chainmaker.yml

        xsed "s%{net_port}%$(($P2P_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{rpc_port}%$(($RPC_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{monitor_port}%$(($MONITOR_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{pprof_port}%$(($PPROF_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{trusted_port}%$(($TRUSTED_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{enable_vm_go}%$ENABLE_VM_GO%g" node$i/chainmaker.yml
        xsed "s%{dockervm_container_name}%"${VM_GO_CONTAINER_NAME_PREFIX}$i"%g" node$i/chainmaker.yml
        xsed "s%{vm_go_runtime_port}%$(($VM_GO_RUNTIME_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{vm_go_engine_port}%$(($VM_GO_ENGINE_PORT+$i-1))%g" node$i/chainmaker.yml
        xsed "s%{vm_go_log_level}%$VM_GO_LOG_LEVEL%g" node$i/chainmaker.yml
        xsed "s%{vm_go_protocol}%$VM_GO_TRANSPORT_PROTOCOL%g" node$i/chainmaker.yml

        system=$(uname)

        if [ "${system}" = "Linux" ]; then
            for ((k = $NODE_CNT; k > 0; k = k - 1)); do
                xsed "/  seeds:/a\    - \"/ip4/127.0.0.1/tcp/$(($P2P_PORT+$k-1))/p2p/{org${k}_peerid}\"" node$i/chainmaker.yml
            done
        else
            ver=$(sw_vers | grep ProductVersion | cut -d':' -f2 | sed 's/\t//g')
            version=${ver:0:2}
            if [ $version -ge 11 ]; then
                for ((k = $NODE_CNT; k > 0; k = k - 1)); do
                xsed  "/  seeds:/a\\
    - \"/ip4/127.0.0.1/tcp/$(($P2P_PORT+$k-1))/p2p/{org${k}_peerid}\"\\
" node$i/chainmaker.yml
                done
            else
                for ((k = $NODE_CNT; k > 0; k = k - 1)); do
                  xsed  "/  seeds:/a\\
                  \ \ \ \ - \"/ip4/127.0.0.1/tcp/$(($P2P_PORT+$k-1))/p2p/{org${k}_peerid}\"\\
                  " node$i/chainmaker.yml
                done
            fi
        fi

        for ((j = 1; j < $CHAIN_CNT + 1; j = j + 1)); do
            xsed "s%#\(.*\)- chainId: chain${j}%\1- chainId: chain${j}%g" node$i/chainmaker.yml
            xsed "s%#\(.*\)genesis: ../config/{org_path$j}/chainconfig/bc${j}.yml%\1genesis: ../config/{org_path$j}/chainconfig/bc${j}.yml%g" node$i/chainmaker.yml

            if  [ $NODE_CNT -eq 1 ]; then
                if [ $CONSENSUS_TYPE -eq 0 ]; then
                    cp $CONFIG_TPL_PATH/chainconfig/bc_solo.tpl node$i/chainconfig/bc$j.yml
                    xsed "s%{consensus_type}%0%g" node$i/chainconfig/bc$j.yml
                else
                    cp $CONFIG_TPL_PATH/chainconfig/bc_solo.tpl node$i/chainconfig/bc$j.yml
                    xsed "s%{consensus_type}%$CONSENSUS_TYPE%g" node$i/chainconfig/bc$j.yml
                fi
            elif [ $NODE_CNT -eq 4 ] || [ $NODE_CNT -eq 7 ]; then
                cp $CONFIG_TPL_PATH/chainconfig/bc_4_7.tpl node$i/chainconfig/bc$j.yml
                xsed "s%{consensus_type}%$CONSENSUS_TYPE%g" node$i/chainconfig/bc$j.yml
            elif [ $NODE_CNT -eq 16 ]; then
                cp $CONFIG_TPL_PATH/chainconfig/bc_16.tpl node$i/chainconfig/bc$j.yml
                xsed "s%{consensus_type}%$CONSENSUS_TYPE%g" node$i/chainconfig/bc$j.yml
            else
                cp $CONFIG_TPL_PATH/chainconfig/bc_10_13.tpl node$i/chainconfig/bc$j.yml
                xsed "s%{consensus_type}%$CONSENSUS_TYPE%g" node$i/chainconfig/bc$j.yml
            fi

            xsed "s%{chain_id}%chain$j%g" node$i/chainconfig/bc$j.yml
            xsed "s%{version}%$VERSION%g" node$i/chainconfig/bc$j.yml
            xsed "s%{org_top_path}%$file%g" node$i/chainconfig/bc$j.yml

            if  [ $NODE_CNT -eq 7 ] || [ $NODE_CNT -eq 13 ] || [ $NODE_CNT -eq 16 ]; then
                xsed "s%#\(.*\)- org_id:%\1- org_id:%g" node$i/chainconfig/bc$j.yml
                xsed "s%#\(.*\)node_id:%\1node_id:%g" node$i/chainconfig/bc$j.yml
                xsed "s%#\(.*\)address:%\1address:%g" node$i/chainconfig/bc$j.yml
                xsed "s%#\(.*\)root:%\1root:%g" node$i/chainconfig/bc$j.yml
                xsed "s%#\(.*\)- \"%\1- \"%g" node$i/chainconfig/bc$j.yml

                # dpos cancel kv annotation
                if  [ $CONSENSUS_TYPE -eq 5 ]; then
                    xsed "s%#\(.*\)- key:%\1- key:%g" node$i/chainconfig/bc$j.yml
                    xsed "s%#\(.*\)value:%\1value:%g" node$i/chainconfig/bc$j.yml
                fi
            fi

            # dpos update erc20.total and epochValidatorNum
            if [ $CONSENSUS_TYPE -eq 5 ]; then
               TOTAL=$(($NODE_CNT*2500000))
               xsed "s%{erc20_total}%$TOTAL%g" node$i/chainconfig/bc$j.yml
               xsed "s%{epochValidatorNum}%$NODE_CNT%g" node$i/chainconfig/bc$j.yml
            fi

            if [ $NODE_CNT -eq 4 ] || [ $NODE_CNT -eq 7 ]; then
              xsed "${BC_YML_TRUST_ROOT_LINE},${BC_YML_TRUST_ROOT_LINE_END}d" node$i/chainconfig/bc$j.yml
            fi
            echo "begin node$i chain$j cert config..."

            c=0
            for file in `ls -tr $BUILD_CRYPTO_CONFIG_PATH`
            do
                c=$(($c+1))
                xsed "s%{org${c}_id}%$file%g" node$i/chainconfig/bc$j.yml

                peerId=`cat $BUILD_CRYPTO_CONFIG_PATH/$file/node/consensus1/consensus1.nodeid`
                xsed "s%{org${c}_peerid}%$peerId%g" node$i/chainconfig/bc$j.yml

                # dpos modify node address
                if  [ $CONSENSUS_TYPE -eq 5 ]; then
                    peerAddr=`cat $BUILD_CRYPTO_CONFIG_PATH/$file/user/client1/client1.addr`
                    xsed "s%{org${c}_peeraddr}%$peerAddr%g" node$i/chainconfig/bc$j.yml
                fi

                if  [ $j -eq 1 ]; then
                    xsed "s%{org${c}_peerid}%$peerId%g" node$i/chainmaker.yml
                fi
                # cp ca
                mkdir -p $BUILD_CONFIG_PATH/node$i/certs/ca/$file
                cp $BUILD_CRYPTO_CONFIG_PATH/$file/ca/ca.crt $BUILD_CONFIG_PATH/node$i/certs/ca/$file

                if  [ $c -eq $i ]; then
                    if [ $c -gt $NODE_CNT ]; then
                      xsed "s%{node_cert_path}%node\/common1\/common1.sign%g" node$i/chainmaker.yml
                      xsed "s%{net_cert_path}%node\/common1\/common1.tls%g" node$i/chainmaker.yml
                      xsed "s%{rpc_cert_path}%node\/common1\/common1.tls%g" node$i/chainmaker.yml
                    else
                      xsed "s%{node_cert_path}%node\/consensus1\/consensus1.sign%g" node$i/chainmaker.yml
                      xsed "s%{net_cert_path}%node\/consensus1\/consensus1.tls%g" node$i/chainmaker.yml
                      xsed "s%{rpc_cert_path}%node\/consensus1\/consensus1.tls%g" node$i/chainmaker.yml
                    fi
                    xsed "s%{org_path}%$file%g" node$i/chainconfig/bc$j.yml
                    xsed "s%{node_cert_path}%node\/consensus1\/consensus1.sign%g" node$i/chainmaker.yml
                    xsed "s%{net_cert_path}%node\/consensus1\/consensus1.tls%g" node$i/chainmaker.yml
                    xsed "s%{rpc_cert_path}%node\/consensus1\/consensus1.tls%g" node$i/chainmaker.yml
                    xsed "s%{org_id}%$file%g" node$i/chainmaker.yml
                    xsed "s%{org_path}%$file%g" node$i/chainmaker.yml
                    xsed "s%{org_path$j}%$file%g" node$i/chainmaker.yml

                    cp -r $BUILD_CRYPTO_CONFIG_PATH/$file/node $BUILD_CONFIG_PATH/node$i/certs
                    cp -r $BUILD_CRYPTO_CONFIG_PATH/$file/user $BUILD_CONFIG_PATH/node$i/certs
                fi

            done
        done

        echo "begin node$i trust config..."
        if  [ $NODE_CNT -eq 4 ] || [ $NODE_CNT -eq 7 ]; then
          trust_path=""
          c=0
          for file in `ls -tr $BUILD_CRYPTO_CONFIG_PATH`
          do
            c=$(($c+1))
            if  [ $c -eq $i ]; then
              trust_path=$file
              break
            fi
          done
          for ((k = 1; k < $CHAIN_CNT + 1; k = k + 1)); do
            for file in `ls -tr $BUILD_CRYPTO_CONFIG_PATH`
            do
                org_id_tmp="\ - org_id: \"${file}\""
                org_root="\ \ \ root:"
                org_root_tmp="\ \ \ \ \ - \"../config/${trust_path}/certs/ca/${file}/ca.crt\""
                if [ "${system}" = "Linux" ]; then
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\ ${org_root_tmp}" node$i/chainconfig/bc$k.yml
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\ ${org_root}" node$i/chainconfig/bc$k.yml
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\ ${org_id_tmp}"   node$i/chainconfig/bc$k.yml
                else
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\\
\ ${org_root_tmp}\\
" node$i/chainconfig/bc$k.yml
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\\
\ ${org_root}\\
"  node$i/chainconfig/bc$k.yml
                  xsed "${BC_YML_TRUST_ROOT_LINE}i\\
\ ${org_id_tmp}\\
"    node$i/chainconfig/bc$k.yml
                fi
            done
          done
        fi

    done
}

check_params
generate_certs
generate_config $@
