/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/logger/v2"
	acPb "chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	storePb "chainmaker.org/chainmaker/pb-go/v2/store"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/store/v2"
	"chainmaker.org/chainmaker/store/v2/conf"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/spf13/cobra"
)

var chainId = "chain1"
var defaultSysContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
var userContractName = "contract1"

func main() {
	if len(os.Args) <= 1 {
		testStorePerformance()
		return
	}
	//testType := os.Args[1]
	//if testType == "1" {
	//	testBlockDBPerformance()
	//}
}

// 测试存储性能
func testStorePerformance() {
	fmt.Println("start block store performance test.")
	mainCmd := &cobra.Command{Use: "chainmaker"}
	// 初始化配置文件
	err := localconf.InitLocalConfig(mainCmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("load config")
	// 创建配置对象
	config, err := conf.NewStorageConfig(localconf.ChainMakerConfig.StorageConfig)
	//err = mapstructure.Decode(localconf.ChainMakerConfig.StorageConfig, &config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("init store")
	l := logger.GetLoggerByChain(logger.MODULE_STORAGE, chainId)
	factory := store.NewFactory()
	bcstore, err := factory.NewStore(chainId, config, l, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("start put block...")
	// 初始化创世块
	initGenesis(bcstore)
	totalStart := time.Now()
	loopNumber := 100
	txCountPerBlock := 10000
	// 循环 构造区块和读写集，完成区块写入
	for i := 0; i < loopNumber; i++ {
		block := createBlock(chainId, uint64(i+1), txCountPerBlock, 200)
		rwset := getTxRWSets(block)
		start := time.Now()
		err1 := bcstore.PutBlock(block, rwset)
		if err1 != nil {
			fmt.Println(err1)
			os.Exit(1)
		}
		testQueryPerformance(bcstore, l, txCountPerBlock)
		if i%100 == 0 {
			l.Infof("write block[%d] include tx(10000) to db, cost:%v", i, time.Since(start))
			fmt.Println("put block:", i, "time:", time.Now())
		}
	}
	seconds := time.Since(totalStart).Seconds()
	l.Infof("total put block count=%d, cost:%v, avg tps:%d",
		loopNumber*txCountPerBlock, time.Since(totalStart), loopNumber*txCountPerBlock/int(seconds))
	fmt.Println("Avg TPS:", loopNumber*txCountPerBlock/int(seconds))
}
func testQueryPerformance(bcstore protocol.BlockchainStore, l protocol.Logger, queryCount int) {
	wg := sync.WaitGroup{}
	wg.Add(queryCount)
	start := time.Now()
	for x := 0; x < queryCount; x++ {
		go func(i int) {
			defer wg.Done()
			bcstore.ReadObject("userContract1", []byte("Key12345678901234567890123456789012345678901234567890_"+strconv.Itoa(i)))
		}(x)
	}
	wg.Wait()
	l.Infof("%v\t read state count=%d\t spend time:%v\tQPS:%v\n", time.Now(), 5000, time.Since(start), float64(5000)/time.Since(start).Seconds())
}

// 创建区块
func createBlock(chainId string, height uint64, txNum int, dataSize int) *commonPb.Block {
	block := &commonPb.Block{
		Header: &commonPb.BlockHeader{
			ChainId:     chainId,
			BlockHeight: height,
			Proposer: &acPb.Member{
				OrgId:      "org1",
				MemberInfo: []byte("User1"),
			},
		},
		Txs: []*commonPb.Transaction{},
	}
	data := make([]byte, dataSize)
	for i := 0; i < txNum; i++ {
		tx := &commonPb.Transaction{
			Payload: &commonPb.Payload{
				ChainId: chainId,
				TxType:  commonPb.TxType_INVOKE_CONTRACT,
				TxId:    generateTxId(chainId, height, i),
				Parameters: []*commonPb.KeyValuePair{&commonPb.KeyValuePair{
					Key:   "evidence",
					Value: data,
				}},
			},
			Sender: &commonPb.EndorsementEntry{
				Signer: &acPb.Member{
					OrgId:      "org1",
					MemberInfo: []byte("User1"),
				},
				Signature: []byte("signature1"),
			},
			Result: &commonPb.Result{
				Code: commonPb.TxStatusCode_SUCCESS,
				ContractResult: &commonPb.ContractResult{
					Result: []byte("ok"),
				},
			},
		}
		block.Txs = append(block.Txs, tx)
	}
	block.Header.BlockHash = generateBlockHash(chainId, height)
	block.Txs[0].Payload.TxId = generateTxId(chainId, height, 0)

	return block
}

//生成一个区块的hash
func generateBlockHash(chainId string, height uint64) []byte {
	blockHash := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", chainId, height)))
	return blockHash[:]
}

// 生成一个txid
func generateTxId(chainId string, height uint64, index int) string {
	txIdBytes := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", chainId, height, index)))
	return hex.EncodeToString(txIdBytes[:])
}

// 创建一个合约payload
func createContractMgrPayload(txId string) *commonPb.Payload {
	p, _ := utils.GenerateInstallContractPayload(userContractName, "2.0",
		commonPb.RuntimeType_WASMER, []byte("byte code!!!"), nil)
	p.TxId = txId
	return p
}

//构造一个配置区块
func createConfigBlock(chainId string, height uint64) *commonPb.Block {
	block := &commonPb.Block{
		Header: &commonPb.BlockHeader{
			ChainId:     chainId,
			BlockHeight: height,
			Proposer: &acPb.Member{
				OrgId:      "org1",
				MemberInfo: []byte("User1"),
			},
		},
		Txs: []*commonPb.Transaction{
			{
				Payload: &commonPb.Payload{
					ChainId:      chainId,
					TxType:       commonPb.TxType_INVOKE_CONTRACT,
					ContractName: syscontract.SystemContract_CHAIN_CONFIG.String(),
					TxId:         generateTxId(chainId, height, 0),
				},
				Sender: &commonPb.EndorsementEntry{
					Signer: &acPb.Member{
						OrgId:      "org1",
						MemberInfo: []byte("User1"),
					},
					Signature: []byte("signature1"),
				},
				Result: &commonPb.Result{
					Code: commonPb.TxStatusCode_SUCCESS,
					ContractResult: &commonPb.ContractResult{
						Result: []byte("ok"),
					},
				},
			},
		},
	}

	block.Header.BlockHash = generateBlockHash(chainId, height)
	block.Txs[0].Payload.TxId = generateTxId(chainId, height, 0)
	return block
}

// 创建区块和读写集
func createInitContractBlockAndRWSets(chainId string, height uint64) (*commonPb.Block, []*commonPb.TxRWSet) {
	block := createBlock(chainId, height, 1, 100)
	block.Header.BlockType = commonPb.BlockType_CONTRACT_MGR_BLOCK
	block.Txs[0].Payload = createContractMgrPayload(generateTxId(chainId, height, 0))

	contract := &commonPb.Contract{
		Name:        userContractName,
		Version:     "2.0",
		RuntimeType: commonPb.RuntimeType_WASMER,
		Status:      commonPb.ContractStatus_NORMAL,
		Creator:     &acPb.MemberFull{MemberInfo: []byte("user1")},
	}
	cdata, _ := contract.Marshal()

	var txRWSets []*commonPb.TxRWSet
	//建表脚本在写集
	txRWset := &commonPb.TxRWSet{
		TxId: block.Txs[0].Payload.TxId,
		TxWrites: []*commonPb.TxWrite{
			{
				Key:          utils.GetContractDbKey(userContractName),
				Value:        cdata,
				ContractName: syscontract.SystemContract_CONTRACT_MANAGE.String(),
			},
			{
				Key:          utils.GetContractByteCodeDbKey(userContractName),
				Value:        []byte("byte code!!!"),
				ContractName: syscontract.SystemContract_CONTRACT_MANAGE.String(),
			},
			{
				Key:          nil,
				Value:        []byte("create table t1(name varchar(50) primary key,amount int)"),
				ContractName: userContractName,
			},
		},
	}
	txRWSets = append(txRWSets, txRWset)
	return block, txRWSets
}

// 创建区块和读写集
func createBlockAndRWSets(chainId string, height uint64, txNum int) (*commonPb.Block, []*commonPb.TxRWSet) {
	block := createBlock(chainId, height, txNum, 100)
	var txRWSets []*commonPb.TxRWSet

	for i := 0; i < txNum; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		txRWset := &commonPb.TxRWSet{
			TxId: block.Txs[i].Payload.TxId,
			TxWrites: []*commonPb.TxWrite{
				{
					Key:          []byte(key),
					Value:        []byte(value),
					ContractName: userContractName,
				},
			},
		}
		txRWSets = append(txRWSets, txRWset)
	}

	return block, txRWSets
}

// 初始化创建创世区块
func initGenesis(s protocol.BlockchainStore) {
	genesis := createConfigBlock(chainId, 0)
	g := &storePb.BlockWithRWSet{Block: genesis, TxRWSets: getTxRWSets(genesis)}
	_ = s.InitGenesis(g)
}

// 根据区块获得读写集
func getTxRWSets(block *commonPb.Block) []*commonPb.TxRWSet {
	rwset := []*commonPb.TxRWSet{}
	for i, tx := range block.Txs {
		row := &commonPb.TxRWSet{
			TxId: tx.Payload.TxId,
			TxWrites: []*commonPb.TxWrite{
				//{
				//	Key:          []byte(fmt.Sprintf("key%d", i)),
				//	Value:        []byte("value1"),
				//	ContractName: userContractName,
				//},
				{
					Key:          []byte(fmt.Sprintf("Tx_%s", tx.Payload.TxId)),
					Value:        []byte(fmt.Sprintf("value[%d]_%s", i, tx.Payload.TxId)),
					ContractName: userContractName,
				},
			},
		}
		rwset = append(rwset, row)
	}
	return rwset
}
