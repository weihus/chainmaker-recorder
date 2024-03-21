/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	acPb "chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	storePb "chainmaker.org/chainmaker/pb-go/v2/store"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"
	"chainmaker.org/chainmaker/store/v2/archive"
	"chainmaker.org/chainmaker/store/v2/binlog"
	"chainmaker.org/chainmaker/store/v2/blockdb"
	"chainmaker.org/chainmaker/store/v2/blockdb/blockfiledb"
	"chainmaker.org/chainmaker/store/v2/conf"
	"chainmaker.org/chainmaker/store/v2/serialization"
	"chainmaker.org/chainmaker/store/v2/statedb"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var chainId = "ut1"

//var dbType = types.MySQL
//var dbType = types.LevelDb
func getFactory() *Factory {
	f := NewFactory()
	_ = f.ioc.Register(func() protocol.Logger { return &test.GoLogger{} })
	_ = f.ioc.Register(func() binlog.BinLogger { return binlog.NewMemBinlog(&test.GoLogger{}) })
	return f
}

var defaultSysContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
var userContractName = "contract1"
var block0 = createConfigBlock(chainId, 0)
var block1 = createBlock(chainId, 1, 1)
var block5 = createBlock(chainId, 5, 1)

func getTxRWSets() []*commonPb.TxRWSet {
	return []*commonPb.TxRWSet{
		{
			//TxId: "abcdefg",
			TxWrites: []*commonPb.TxWrite{
				{
					Key:          []byte("key1"),
					Value:        []byte("value1"),
					ContractName: defaultSysContractName,
				},
				{
					Key:          []byte("key2"),
					Value:        []byte("value2"),
					ContractName: defaultSysContractName,
				},
				{
					Key:          []byte("key3"),
					Value:        nil,
					ContractName: defaultSysContractName,
				},
			},
		},
	}
}

var config1 = getSqliteConfig()

//var config3 = getlvldbConfig("")

//var configBadger = getBadgerConfig("")
func getLocalMySqlConfig() *conf.StorageConfig {
	conf1, _ := conf.NewStorageConfig(nil)
	conf1.DbPrefix = "org1_"
	conf1.StorePath = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))
	conf1.WriteBlockType = 0
	var sqlconfig = make(map[string]interface{})
	sqlconfig["sqldb_type"] = "mysql"
	sqlconfig["dsn"] = "root:123@tcp(127.0.0.1:3306)/mysql"

	dbConfig := &conf.DbConfig{
		Provider:    "sql",
		SqlDbConfig: sqlconfig,
	}
	statedbConfig := &conf.DbConfig{
		Provider:    "sql",
		SqlDbConfig: sqlconfig,
	}

	lvlConfig := make(map[string]interface{})
	lvlConfig["store_path"] = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))

	txExistDbConfig := &conf.DbConfig{
		Provider:      "leveldb",
		LevelDbConfig: lvlConfig,
	}
	conf1.DisableBlockFileDb = true
	conf1.BlockDbConfig = dbConfig
	conf1.StateDbConfig = statedbConfig
	conf1.HistoryDbConfig = conf.NewHistoryDbConfig(dbConfig)
	conf1.ResultDbConfig = dbConfig
	conf1.ContractEventDbConfig = dbConfig
	conf1.DisableContractEventDB = true
	conf1.TxExistDbConfig = txExistDbConfig
	conf1.LogDBSegmentSize = 20
	return conf1
}
func getSqliteConfig() *conf.StorageConfig {
	conf1, _ := conf.NewStorageConfig(nil)
	conf1.StorePath = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))
	conf1.WriteBlockType = 0
	var sqlconfig = make(map[string]interface{})
	sqlconfig["sqldb_type"] = "sqlite"
	sqlconfig["dsn"] = ":memory:"

	dbConfig := &conf.DbConfig{
		Provider:    "sql",
		SqlDbConfig: sqlconfig,
	}
	statedbConfig := &conf.DbConfig{
		Provider:    "sql",
		SqlDbConfig: sqlconfig,
	}

	lvlConfig := make(map[string]interface{})
	lvlConfig["store_path"] = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))

	txExistDbConfig := &conf.DbConfig{
		Provider:      "leveldb",
		LevelDbConfig: lvlConfig,
	}
	conf1.DisableBlockFileDb = true
	conf1.BlockDbConfig = dbConfig
	conf1.StateDbConfig = statedbConfig
	conf1.HistoryDbConfig = conf.NewHistoryDbConfig(dbConfig)
	conf1.ResultDbConfig = dbConfig
	conf1.ContractEventDbConfig = dbConfig
	conf1.DisableContractEventDB = true
	conf1.TxExistDbConfig = txExistDbConfig
	conf1.LogDBSegmentSize = 20
	//conf1.DisableBigFilter = true
	conf1.RollingWindowCacheCapacity = 1000000
	return conf1
}
func getBadgerConfig(path string) *conf.StorageConfig {
	conf1, _ := conf.NewStorageConfig(nil)
	if path == "" {
		path = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))
	}
	conf1.StorePath = path
	conf1.WriteBlockType = 1

	badgerConfig := make(map[string]interface{})
	badgerConfig["store_path"] = path
	dbConfig := &conf.DbConfig{
		Provider:       "badgerdb",
		BadgerDbConfig: badgerConfig,
	}
	conf1.BlockDbConfig = dbConfig
	conf1.StateDbConfig = dbConfig
	conf1.HistoryDbConfig = conf.NewHistoryDbConfig(dbConfig)
	conf1.ResultDbConfig = dbConfig
	conf1.DisableContractEventDB = true
	conf1.TxExistDbConfig = dbConfig
	conf1.StorePath = filepath.Join(os.TempDir(), fmt.Sprintf("%d_wal", time.Now().Nanosecond()))
	return conf1
}

func getlvldbConfig(path string) *conf.StorageConfig {
	conf1, _ := conf.NewStorageConfig(nil)
	if path == "" {
		path = filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))
	}

	conf1.StorePath = path
	conf1.WriteBlockType = 0

	lvlConfig := make(map[string]interface{})
	lvlConfig["store_path"] = path

	path2 := filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Nanosecond()))

	lvlConfig2 := make(map[string]interface{})
	lvlConfig2["store_path"] = path2

	dbConfig := &conf.DbConfig{
		Provider:      "leveldb",
		LevelDbConfig: lvlConfig,
	}
	dbConfig2 := &conf.DbConfig{
		Provider:      "leveldb",
		LevelDbConfig: lvlConfig2,
	}
	conf1.BlockDbConfig = dbConfig
	conf1.StateDbConfig = dbConfig
	conf1.HistoryDbConfig = conf.NewHistoryDbConfig(dbConfig)
	conf1.ResultDbConfig = dbConfig
	conf1.DisableContractEventDB = true
	conf1.TxExistDbConfig = dbConfig2
	conf1.DisableBlockFileDb = false
	//conf1.DisableBigFilter = true
	conf1.StorePath = filepath.Join(os.TempDir(), fmt.Sprintf("%d_wal", time.Now().Nanosecond()))
	return conf1
}
func generateBlockHash(chainId string, height uint64) []byte {
	blockHash := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", chainId, height)))
	return blockHash[:]
}

func generateTxId(chainId string, height uint64, index int) string {
	txIdBytes := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", chainId, height, index)))
	return hex.EncodeToString(txIdBytes[:])
}

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

func createBlock(chainId string, height uint64, txNum int) *commonPb.Block {
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
	for i := 0; i < txNum; i++ {
		tx := &commonPb.Transaction{
			Payload: &commonPb.Payload{
				ChainId: chainId,
				TxType:  commonPb.TxType_INVOKE_CONTRACT,
				TxId:    generateTxId(chainId, height, i),
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

func createConfBlock(chainId string, height uint64) *commonPb.Block {
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
					TxId:         generateTxId(chainId, height, 0),
					ContractName: syscontract.SystemContract_CHAIN_CONFIG.String(),
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

func createContractMgrPayload(txId string) *commonPb.Payload {
	p, _ := utils.GenerateInstallContractPayload(userContractName, "2.0",
		commonPb.RuntimeType_WASMER, []byte("byte code!!!"), nil)
	p.TxId = txId
	return p
}
func createInitContractBlockAndRWSets(chainId string, height uint64) (*commonPb.Block, []*commonPb.TxRWSet) {
	block := createBlock(chainId, height, 1)
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

func createBlockAndRWSets(chainId string, height uint64, txNum int) (*commonPb.Block, []*commonPb.TxRWSet) {
	block := createBlock(chainId, height, txNum)
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

func createConfBlockAndRWSets(chainId string, height uint64) (*commonPb.Block, []*commonPb.TxRWSet) {
	block := createConfBlock(chainId, height)
	txRWSets := []*commonPb.TxRWSet{
		{
			TxId: block.Txs[0].Payload.TxId,
			TxWrites: []*commonPb.TxWrite{
				{
					Key:          []byte("key_0"),
					Value:        []byte("value_0"),
					ContractName: defaultSysContractName,
				},
			},
		},
	}

	return block, txRWSets
}

//var log = &test.GoLogger{}

//func TestMain(m *testing.M) {
//	fmt.Println("begin")
//	if dbType == types.MySQL {
//		// drop mysql table
//		conf := &localconf.ChainMakerConfig.StorageConfig
//		conf.Provider = "MySQL"
//		conf.MysqlConfig.Dsn = "root:123456@tcp(127.0.0.1:3306)/"
//		db, err := blocksqldb.NewBlockSqlDB(chainId, log)
//		if err != nil {
//			panic("faild to open mysql")
//		}
//		// clear data
//		gormDB := db.(*blocksqldb.BlockSqlDB).GetDB()
//		gormDB.Migrator().DropTable(&blocksqldb.BlockInfo{})
//		gormDB.Migrator().DropTable(&blocksqldb.TxInfo{})
//		gormDB.Migrator().DropTable(&statesqldb.StateInfo{})
//		gormDB.Migrator().DropTable(&historysqldb.HistoryInfo{})
//	}
//	os.RemoveAll(chainId)
//	m.Run()
//	fmt.Println("end")
//}
func Test_blockchainStoreImpl_GetBlockSqlDb(t *testing.T) {
	testBlockchainStoreImpl_GetBlock(t, config1)
}
func Test_blockchainStoreImpl_GetBlockLevelDb(t *testing.T) {
	testBlockchainStoreImpl_GetBlock(t, getlvldbConfig(""))
}

//TODO Devin
//func Test_blockchainStoreImpl_GetBlockBadgerdb(t *testing.T) {
//	testBlockchainStoreImpl_GetBlock(t, getBadgerConfig(""))
//}

func testBlockchainStoreImpl_GetBlock(t *testing.T, config *conf.StorageConfig) {
	var funcName = "get block"
	tests := []struct {
		name  string
		block *commonPb.Block
	}{
		{funcName, createBlock(chainId, 1, 1)},
		{funcName, createBlock(chainId, 2, 1)},
		{funcName, createBlock(chainId, 3, 1)},
		{funcName, createBlock(chainId, 4, 1)},
	}
	var factory = getFactory()
	s, err := factory.NewStore(chainId, config, &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {
		s.Close()
	}(s)
	initGenesis(s)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err1 := s.PutBlock(tt.block, nil); err1 != nil {
				t.Errorf("blockchainStoreImpl.PutBlock(), error %v", err)
			}
			got, err1 := s.GetBlockByHash(tt.block.Header.BlockHash)
			assert.Nil(t, err1)
			assert.NotNil(t, got)
			assert.Equal(t, tt.block.String(), got.String())
		})
	}
}
func initGenesis(s protocol.BlockchainStore) {
	genesis := block0
	g := &storePb.BlockWithRWSet{Block: genesis, TxRWSets: getTxRWSets()}
	_ = s.InitGenesis(g)
}

func Test_blockchainStoreImpl_PutBlock(t *testing.T) {
	var factory = getFactory()
	//s, err := factory.newStore(chainId, config1)
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	initGenesis(s)
	txRWSets := getTxRWSets()
	if err != nil {
		panic(err)
	}
	defer func() {
		time.Sleep(1 * time.Second)
		s.Close()
	}()
	txRWSets[0].TxId = block5.Txs[0].Payload.TxId
	err = s.PutBlock(block5, txRWSets)
	assert.NotNil(t, err)
}

//测试高速写模式,用kvdb
func Test_blockchainStoreImpl_PutBlock2(t *testing.T) {
	var factory = getFactory()
	//高速写
	testConf := getlvldbConfig("")
	testConf.WriteBlockType = 1
	s, err := factory.NewStore(chainId, testConf, &test.GoLogger{}, nil) //newStore(chainId, testConf)
	initGenesis(s)
	txRWSets := getTxRWSets()
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {
		time.Sleep(1 * time.Second)
		s.Close()
	}(s)
	txRWSets[0].TxId = block1.Txs[0].Payload.TxId
	err = s.PutBlock(block1, txRWSets)
	assert.Nil(t, err)
}

//测试配置出错 WriteBlockType 参数
func Test_blockchainStoreImpl_PutBlock3(t *testing.T) {
	var factory = getFactory()
	//高速写, 故意配置成2,模拟配置出错
	var configPutBlock3 = getlvldbConfig("")
	configPutBlock3.WriteBlockType = 2
	s, err := factory.NewStore(chainId, configPutBlock3, &test.GoLogger{}, nil) //factory.newStore(chainId, configPutBlock3)
	initGenesis(s)
	txRWSets := getTxRWSets()
	if err != nil {
		panic(err)
	}
	defer func() {
		time.Sleep(1 * time.Second)
		s.Close()
	}()
	txRWSets[0].TxId = block1.Txs[0].Payload.TxId
	err = s.PutBlock(block1, txRWSets)
	if err != nil && err.Error() != "config error,write_block_type: "+
		strconv.Itoa(configPutBlock3.WriteBlockType) {
		t.Errorf("err = %v, want %v", err, "config error,write_block_type: 0")
	}
}

// 测试tmp bfdb file
func Test_blockchainStoreImpl_PutBlock4(t *testing.T) {
	var factory = getFactory()
	var configPutBlock4 = getlvldbConfig("")
	configPutBlock4.DisableBlockFileDb = false
	configPutBlock4.WriteBlockType = 0
	configPutBlock4.BlockStoreTmpPath = fmt.Sprintf("%s_tmp", configPutBlock4.StorePath)
	configPutBlock4.LogDBSegmentSize = 10
	s, err := factory.NewStore(chainId, configPutBlock4, &test.GoLogger{}, nil) //factory.newStore(chainId, configPutBlock3)
	initGenesis(s)
	if err != nil {
		panic(err)
	}
	defer func() {
		time.Sleep(1 * time.Second)
		s.Close()
	}()

	totalBlocks := 20
	data := "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	for i := 1; i <= totalBlocks; i++ {
		b, rw := createBlockAndRWSets(chainId, uint64(i), 1000)
		for rwi := 0; rwi < len(rw); rwi++ {
			for txwi := 0; txwi < len(rw[rwi].TxWrites); txwi++ {
				rw[rwi].TxWrites[txwi].Value = []byte(data)
			}
		}

		err = s.PutBlock(b, rw)
		assert.Nil(t, err)
	}

	// wait fdb move finished
	time.Sleep(12 * time.Second)

	// get block located in normal bfdb file
	_, err = s.GetBlock(1)
	assert.Nil(t, err)

	// get block located in tmp bfdb file
	_, err = s.GetBlock(uint64(totalBlocks))
	assert.Nil(t, err)
}

func Test_blockchainStoreImpl_HasBlock(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getlvldbConfig(""), &test.GoLogger{}, nil) //factory.newStore(chainId, getlvldbConfig(""))
	if err != nil {
		panic(err)
	}
	defer func() {
		time.Sleep(1 * time.Second)
		s.Close()
	}()
	initGenesis(s)
	exist, _ := s.BlockExists(block0.Header.BlockHash)
	assert.True(t, exist)

	exist, err = s.BlockExists([]byte("not exist"))
	assert.Equal(t, nil, err)
	assert.False(t, exist)
}

//初始化数据库：0创世区块，1合约创建区块，2-5合约调用区块
func init5Blocks(s protocol.BlockchainStore) {
	genesis := &storePb.BlockWithRWSet{Block: block0, TxRWSets: getTxRWSets()}
	err := s.InitGenesis(genesis)
	if err != nil {
		panic(err)
	}
	b, rw := createInitContractBlockAndRWSets(chainId, 1)
	err = s.PutBlock(b, rw)
	if err != nil {
		panic(err)
	}
	b, rw = createBlockAndRWSets(chainId, 2, 2)
	err = s.PutBlock(b, rw)
	if err != nil {
		panic(err)
	}
	b, rw = createBlockAndRWSets(chainId, 3, 3)
	err = s.PutBlock(b, rw)
	if err != nil {
		panic(err)
	}
	b, rw = createBlockAndRWSets(chainId, 4, 10)
	err = s.PutBlock(b, rw)
	if err != nil {
		panic(err)
	}
	b, _ = createBlockAndRWSets(chainId, 5, 1)
	err = s.PutBlock(b, getTxRWSets())
	if err != nil {
		panic(err)
	}
}
func init5ContractBlocks(s protocol.BlockchainStore) []*commonPb.Block {
	result := []*commonPb.Block{}
	genesis := &storePb.BlockWithRWSet{Block: block0}
	genesis.TxRWSets = []*commonPb.TxRWSet{
		{
			TxWrites: []*commonPb.TxWrite{
				{
					Key:          []byte("key1"),
					Value:        []byte("value1"),
					ContractName: syscontract.SystemContract_CHAIN_CONFIG.String(),
				},
			},
		},
	}

	_ = s.InitGenesis(genesis)
	result = append(result, genesis.Block)
	b, rw := createInitContractBlockAndRWSets(chainId, 1)
	fmt.Println("Is contract?", utils.IsContractMgmtBlock(b))
	_ = s.PutBlock(b, rw)
	result = append(result, b)
	b, rw = createBlockAndRWSets(chainId, 2, 2)
	_ = s.PutBlock(b, rw)
	result = append(result, b)
	b, rw = createBlockAndRWSets(chainId, 3, 3)
	_ = s.PutBlock(b, rw)
	result = append(result, b)
	b, rw = createBlockAndRWSets(chainId, 4, 10)
	_ = s.PutBlock(b, rw)
	result = append(result, b)
	b, rw = createBlockAndRWSets(chainId, 5, 1)
	_ = s.PutBlock(b, rw)
	result = append(result, b)
	return result
}
func Test_blockchainStoreImpl_GetBlockAt(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		time.Sleep(1 * time.Second)
		s.Close()
	}()
	init5Blocks(s)
	got, err := s.GetBlock(block5.Header.BlockHeight)
	assert.Equal(t, nil, err)
	assert.Equal(t, block5.String(), got.String())
}

func Test_blockchainStoreImpl_GetLastBlock(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {

		s.Close()
	}(s)
	init5Blocks(s)
	assert.Equal(t, nil, err)
	lastBlock, err := s.GetLastBlock()
	assert.Equal(t, nil, err)
	assert.Equal(t, block5.Header.BlockHeight, lastBlock.Header.BlockHeight)
}

func Test_blockchainStoreImpl_GetBlockByTx(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {

		s.Close()
	}(s)
	init5Blocks(s)
	block, err := s.GetBlockByTx(generateTxId(chainId, 3, 0))
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(3), block.Header.BlockHeight)

	blockNotExist, err := s.GetBlockByTx("not_exist_txid")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, blockNotExist == nil)

}

const HAS_TX = "has tx"

func Test_blockchainStoreImpl_GetTx(t *testing.T) {
	//funcName := HAS_TX
	//tests := []struct {
	//	name  string
	//	block *commonPb.Block
	//}{
	//	{funcName, createBlock(chainId, 1, 1)},
	//	{funcName, createBlock(chainId, 2, 1)},
	//	{funcName, createBlock(chainId, 3, 1)},
	//	{funcName, createBlock(chainId, 4, 1)},
	//	{funcName, createBlock(chainId, 999999, 2)},
	//}

	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {

		s.Close()
	}(s)
	//assert.DeepEqual(t, s.GetTx(tests[0].block.Txs[0].TxId, )
	blocks := init5ContractBlocks(s)
	tx, err := s.GetTx(blocks[1].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	if tx == nil {
		t.Error("Error, GetTx")
	}
	_, err = s.GetTx(blocks[2].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	_, err = s.GetTx(blocks[3].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	_, err = s.GetTx(blocks[4].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	//assert.Equal(t, tx.Payload.TxId, generateTxId(chainId, 1, 0))
	//
	////chain not exist
	//tx, err = s.GetTx(generateTxId("not exist chain", 1, 0))
	//t.Log(tx)
	//assert.NotNil(t,  err)

}

func Test_blockchainStoreImpl_GetTxWithRWSet(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {
		s.Close()
	}(s)
	//assert.DeepEqual(t, s.GetTx(tests[0].block.Txs[0].TxId, )
	blocks := init5ContractBlocks(s)
	tx, err := s.GetTx(blocks[1].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	if tx == nil {
		t.Error("Error, GetTx")
	}
	txwithRwset, err := s.GetTxWithRWSet(blocks[2].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	t.Log(txwithRwset.Transaction)
	t.Log(txwithRwset.RwSet)
	_, err = s.GetTxWithRWSet(blocks[3].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	txinfoWithRWSet, err := s.GetTxInfoWithRWSet(blocks[4].Txs[0].Payload.TxId)
	assert.Nil(t, err)
	assert.EqualValues(t, 4, txinfoWithRWSet.BlockHeight)

}

func Test_blockchainStoreImpl_HasTx(t *testing.T) {
	funcName := HAS_TX
	tests := []struct {
		name  string
		block *commonPb.Block
	}{
		{funcName, createBlock(chainId, 1, 1)},
		{funcName, createBlock(chainId, 2, 1)},
		{funcName, createBlock(chainId, 3, 1)},
		{funcName, createBlock(chainId, 4, 1)},
		{funcName, createBlock(chainId, 999999, 1)},
	}
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {

		s.Close()
	}(s)
	init5Blocks(s)
	exist, err := s.TxExists(tests[0].block.Txs[0].Payload.TxId)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, exist)
	exist, err = s.TxExists(tests[1].block.Txs[0].Payload.TxId)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, exist)
	exist, err = s.TxExists(tests[2].block.Txs[0].Payload.TxId)
	assert.Equal(t, true, exist)
	assert.Equal(t, nil, err)
	exist, err = s.TxExists(tests[3].block.Txs[0].Payload.TxId)
	assert.Equal(t, true, exist)
	assert.Equal(t, nil, err)
	exist, err = s.TxExists(tests[4].block.Txs[0].Payload.TxId)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, exist)
}
func Test_blockchainStoreImpl_TxExists(t *testing.T) {
	cfg := getSqliteConfig()
	cfg.TxExistDbConfig = nil
	var factory = getFactory()
	s, err := factory.NewStore(chainId, cfg, &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func(s protocol.BlockchainStore) {
		s.Close()
	}(s)
	init5Blocks(s)
	exist, err := s.TxExists(block1.Txs[0].Payload.TxId)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, exist)

}
func Test_blockchainStoreImpl_ReadObject(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	assert.Nil(t, err)
	defer func() { _ = s.Close() }()
	initGenesis(s)
	assert.Equal(t, nil, err)
	value, err := s.ReadObject(defaultSysContractName, []byte("key1"))
	assert.Equal(t, nil, err)
	assert.Equal(t, value, []byte("value1"))

	value, err = s.ReadObject(defaultSysContractName, []byte("key2"))
	assert.Equal(t, nil, err)
	assert.Equal(t, value, []byte("value2"))
}

func Test_blockchainStoreImpl_ReadObjects(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	//s, err := factory.NewStore(chainId, getlvldbConfig(""), &test.GoLogger{}, nil)
	assert.Nil(t, err)
	defer func() { _ = s.Close() }()
	initGenesis(s)
	assert.Equal(t, nil, err)

	keys := make([][]byte, 2)
	values := make([][]byte, 2)
	keys[0] = []byte("key1")
	keys[1] = []byte("key2")
	values[0] = []byte("value1")
	values[1] = []byte("value2")
	valuesR, err1 := s.ReadObjects(defaultSysContractName, keys)
	assert.Equal(t, nil, err1)
	for i := 0; i < len(values); i++ {
		assert.Equal(t, valuesR[i], values[i])
	}
}

func Test_blockchainStoreImpl_SelectObject(t *testing.T) {
	//t.Run("sql iterator", func(tt *testing.T) {
	//	testSelectStateKeyRange(tt, getSqliteConfig())
	//})
	//TODO Devin: sqlite get error
	t.Run("kvdb iterator", func(tt *testing.T) {
		cf := getlvldbConfig("")
		cf.DisableBlockFileDb = false
		testSelectStateKeyRange(tt, cf)
	})
}
func testSelectStateKeyRange(t *testing.T, storeConfig *conf.StorageConfig) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, storeConfig, &test.GoLogger{}, nil)
	defer func() { _ = s.Close() }()
	init5ContractBlocks(s)
	assert.Equal(t, nil, err)

	iter, err := s.SelectObject(userContractName, []byte("key_2"), []byte("key_4"))
	assert.Nil(t, err)
	defer iter.Release()
	var count = 0
	for iter.Next() {
		count++
		kv, e := iter.Value()
		assert.Nil(t, e)
		t.Logf("key:%s, value:%s\n", string(kv.Key), string(kv.Value))
	}
	assert.Equal(t, 2, count)
}
func Test_blockchainStoreImpl_GetHistoryForKey(t *testing.T) {
	t.Run("kvdb key history iterator", func(tt *testing.T) {
		cf := getlvldbConfig("")
		cf.DisableBlockFileDb = false
		testKeyHistory(tt, cf)
	})
	t.Run("sql history iterator", func(tt *testing.T) {
		testKeyHistory(tt, getSqliteConfig())
	})
}
func testKeyHistory(tt *testing.T, cf *conf.StorageConfig) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, cf, &test.GoLogger{}, nil)
	defer func() { _ = s.Close() }()
	init5ContractBlocks(s)
	assert.Equal(tt, nil, err)
	testSelectKeyHistory(tt, s, "key_2", 2)
	testSelectKeyHistory(tt, s, "key_1", 3)
	testSelectKeyHistory(tt, s, "key_0", 4)
}
func testSelectKeyHistory(t *testing.T, s protocol.BlockchainStore, key string, hisCount int) {
	iter, err := s.GetHistoryForKey(userContractName, []byte(key))
	assert.Nil(t, err)
	defer iter.Release()
	var count = 0
	for iter.Next() {
		count++
		kv, e := iter.Value()
		assert.Nil(t, e)
		t.Logf("txid:%s, value:%s,time:%d,block height:%d\n",
			kv.TxId, string(kv.Value), kv.Timestamp, kv.BlockHeight)
	}
	assert.Equal(t, hisCount, count)
}

func Test_blockchainStoreImpl_TxRWSet(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	defer func() { _ = s.Close() }()
	init5Blocks(s)
	assert.Equal(t, nil, err)
	impl, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	txid := block5.Txs[0].Payload.TxId
	txRWSetFromDB, err := impl.GetTxRWSet(txid)
	assert.Equal(t, nil, err)
	t.Log(txRWSetFromDB)
	assert.Equal(t, getTxRWSets()[0].String(), txRWSetFromDB.String())
}

/*func TestBlockStoreImpl_Recovery(t *testing.T) {
	fmt.Println("test recover, please delete DB file in 20s")
	time.Sleep(20 * time.Second)
	s, err := Factory{}.NewStore(dbType, chainId)
	defer s.Close()
	assert.Equal(t, nil, err)
	fmt.Println("recover commpleted")
}*/

func Test_blockchainStoreImpl_getLastSavepoint(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	defer func() { _ = s.Close() }()
	init5Blocks(s)
	assert.Equal(t, nil, err)
	impl, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	height, err := impl.getLastFileSavepoint()
	assert.Nil(t, err)
	assert.Equal(t, uint64(5), height)
	height, _ = impl.blockDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), height)
	height, _ = impl.stateDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), height)
	height, _ = impl.resultDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), height)
	height, _ = impl.historyDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), height)
}

func TestBlockStoreImpl_GetTxRWSetsByHeight(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getlvldbConfig(""), &test.GoLogger{}, nil)
	defer func() { _ = s.Close() }()
	init5Blocks(s)
	txRWSets := getTxRWSets()
	assert.Equal(t, nil, err)
	impl, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	txRWSetsFromDB, err := impl.GetTxRWSetsByHeight(5)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(txRWSets), len(txRWSetsFromDB))
	for index, txRWSet := range txRWSetsFromDB {
		assert.Equal(t, txRWSets[index].String(), txRWSet.String())
	}
}

//func TestBlockStoreImpl_GetDBHandle(t *testing.T) {
//	var factory=getFactory()
//	s, err := factory.newStore(chainId, config1, binlog.NewMemBinlog(), log)
//	defer s.Close()
//	assert.Equal(t, nil, err)
//	dbHandle := s.GetDBHandle("test")
//	dbHandle.Put([]byte("a"), []byte("A"))
//	value, err := dbHandle.Get([]byte("a"))
//	assert.Equal(t, nil, err)
//	assert.Equal(t, []byte("A"), value)
//}

func Test_blockchainStoreImpl_GetBlockWith100Tx(t *testing.T) {
	var factory = getFactory()
	s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = s.Close() }()
	init5Blocks(s)
	block, txRWSets := createBlockAndRWSets(chainId, 6, 1)
	_ = s.PutBlock(block, txRWSets)
	block, txRWSets = createBlockAndRWSets(chainId, 7, 100)
	err = s.PutBlock(block, txRWSets)

	assert.Nil(t, err)
	blockFromDB, err := s.GetBlock(7)
	assert.Equal(t, nil, err)
	assert.Equal(t, block.String(), blockFromDB.String())

	txRWSetsFromDB, err := s.GetTxRWSetsByHeight(7)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(txRWSets), len(txRWSetsFromDB))
	for i := 0; i < len(txRWSets); i++ {
		assert.Equal(t, txRWSets[i].String(), txRWSetsFromDB[i].String())
	}

	blockWithRWSets, err := s.GetBlockWithRWSets(7)
	assert.Equal(t, nil, err)
	assert.Equal(t, block.String(), blockWithRWSets.Block.String())
	for i := 0; i < len(blockWithRWSets.TxRWSets); i++ {
		assert.Equal(t, txRWSets[i].String(), blockWithRWSets.TxRWSets[i].String())
	}
}

func Test_blockchainStoreImpl_recovory(t *testing.T) {
	var factory = getFactory()
	ldbConfig := getlvldbConfig("")
	ldbConfig.DisableBlockFileDb = true
	s, err := factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	//defer s.Close()
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)
	//
	block6, txRWSets6 := createBlockAndRWSets(chainId, 6, 100)

	//1. commit wal
	blockWithRWSet := &storePb.BlockWithRWSet{
		Block:    block6,
		TxRWSets: txRWSets6,
	}
	blockWithRWSetBytes, _, err := serialization.SerializeBlock(blockWithRWSet)
	assert.Equal(t, nil, err)
	_, err = bs.writeBlockToFile(block6.Header.BlockHeight, blockWithRWSetBytes)
	if err != nil {
		fmt.Printf("chain[%s] Failed to write wal, block[%d]",
			block6.Header.ChainId, block6.Header.BlockHeight)
		t.Error(err)
	}
	binlogSavepoint, _ := bs.getLastFileSavepoint()
	assert.EqualValues(t, uint64(6), binlogSavepoint)
	blockDBSavepoint, _ := bs.blockDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), blockDBSavepoint)

	stateDBSavepoint, _ := bs.stateDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), stateDBSavepoint)

	historyDBSavepoint, _ := bs.historyDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), historyDBSavepoint)
	resultDBSavepoint, _ := bs.resultDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), resultDBSavepoint)

	s.Close()
	t.Log("start recovery db from bin log")
	//recovory
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	t.Log("db recovered")
	impl, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	binlogSavepoint, _ = impl.getLastFileSavepoint()
	assert.EqualValues(t, 6, binlogSavepoint)
	blockDBSavepoint, _ = impl.blockDB.GetLastSavepoint()
	assert.EqualValues(t, 6, blockDBSavepoint)

	stateDBSavepoint, _ = impl.stateDB.GetLastSavepoint()
	assert.EqualValues(t, 6, stateDBSavepoint)

	historyDBSavepoint, _ = impl.historyDB.GetLastSavepoint()
	assert.EqualValues(t, 6, historyDBSavepoint)
	resultDBSavepoint, _ = impl.resultDB.GetLastSavepoint()
	assert.EqualValues(t, 6, resultDBSavepoint)
	s.Close()

	//check recover result
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.Nil(t, err)
	blockWithRWSets, err := s.GetBlockWithRWSets(6)
	assert.Equal(t, nil, err)
	assert.Equal(t, block6.String(), blockWithRWSets.Block.String())
	for i := 0; i < len(blockWithRWSets.TxRWSets); i++ {
		assert.Equal(t, txRWSets6[i].String(), blockWithRWSets.TxRWSets[i].String())
	}
	s.Close()
}

func Test_blockchainStoreImpl_recovory_filedb(t *testing.T) {
	var factory = getFactory()
	ldbConfig := getlvldbConfig("")
	ldbConfig.DisableBlockFileDb = false
	s, err := factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	//defer s.Close()
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)

	// put block5 again
	blockWithRWSet := &storePb.BlockWithRWSet{
		Block:    block5,
		TxRWSets: getTxRWSets(),
	}
	blockWithRWSetBytes, _, err := serialization.SerializeBlock(blockWithRWSet)
	assert.Equal(t, nil, err)
	_, err = bs.writeBlockToFile(block5.Header.BlockHeight, blockWithRWSetBytes)
	assert.NotNil(t, err)

	// put block6 again
	block6, txRWSets6 := createBlockAndRWSets(chainId, 6, 100)

	//1. commit wal
	blockWithRWSet = &storePb.BlockWithRWSet{
		Block:    block6,
		TxRWSets: txRWSets6,
	}
	blockWithRWSetBytes, _, err = serialization.SerializeBlock(blockWithRWSet)
	assert.Nil(t, err)
	_, err = bs.writeBlockToFile(block6.Header.BlockHeight, blockWithRWSetBytes)
	if err != nil {
		fmt.Printf("chain[%s] Failed to write file db, block[%d]",
			block6.Header.ChainId, block6.Header.BlockHeight)
		t.Error(err)
	}
	binlogSavepoint, _ := bs.getLastFileSavepoint()
	assert.EqualValues(t, uint64(6), binlogSavepoint)
	blockDBSavepoint, _ := bs.blockDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), blockDBSavepoint)

	stateDBSavepoint, _ := bs.stateDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), stateDBSavepoint)

	historyDBSavepoint, _ := bs.historyDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), historyDBSavepoint)
	resultDBSavepoint, _ := bs.resultDB.GetLastSavepoint()
	assert.Equal(t, uint64(5), resultDBSavepoint)

	assert.Nil(t, s.Close())
	t.Log("start recovery db from bin log")
	//recovory
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	t.Log("db recovered")
	impl, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	binlogSavepoint, _ = impl.getLastFileSavepoint()
	assert.EqualValues(t, 6, binlogSavepoint)
	blockDBSavepoint, _ = impl.blockDB.GetLastSavepoint()
	assert.EqualValues(t, 6, blockDBSavepoint)

	stateDBSavepoint, _ = impl.stateDB.GetLastSavepoint()
	assert.EqualValues(t, 6, stateDBSavepoint)

	historyDBSavepoint, _ = impl.historyDB.GetLastSavepoint()
	assert.EqualValues(t, 6, historyDBSavepoint)
	resultDBSavepoint, _ = impl.resultDB.GetLastSavepoint()
	assert.EqualValues(t, 6, resultDBSavepoint)
	s.Close()

	//check recover result
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.Nil(t, err)
	blockWithRWSets, err := s.GetBlockWithRWSets(6)
	assert.Equal(t, nil, err)
	assert.Equal(t, block6.String(), blockWithRWSets.Block.String())
	for i := 0; i < len(blockWithRWSets.TxRWSets); i++ {
		assert.Equal(t, txRWSets6[i].String(), blockWithRWSets.TxRWSets[i].String())
	}
	s.Close()
}

func Test_blockchainStoreImpl_recovory_filedb_missed(t *testing.T) {
	var factory = getFactory()
	ldbConfig := getlvldbConfig("")
	ldbConfig.DisableBlockFileDb = false
	ldbConfig.DisableHistoryDB = false
	ldbConfig.DisableResultDB = false

	// setup store and put some block
	s, err := factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	//defer s.Close()
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(bs)

	// close store
	assert.Nil(t, s.Close())
	factory.ioc.Reset()

	// move block file db
	bfdbPath := path.Join(ldbConfig.StorePath, chainId, "bfdb")
	bfdbPath1 := bfdbPath + "_backup"
	assert.Nil(t, os.Rename(bfdbPath, bfdbPath1))

	// start store
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.NotNil(t, err)
	factory.ioc.Reset()

	// remove new created bfdb dir and move back the backup block file db
	assert.Nil(t, os.RemoveAll(bfdbPath))
	assert.Nil(t, os.Rename(bfdbPath1, bfdbPath))

	// remove leveldb LOCK file
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.BlockDbConfig.LevelDbConfig["store_path"].(string), chainId, "store_block", "LOCK")))
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.StateDbConfig.LevelDbConfig["store_path"].(string), chainId, "store_state", "LOCK")))
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.HistoryDbConfig.LevelDbConfig["store_path"].(string), chainId, "store_history", "LOCK")))
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.ResultDbConfig.LevelDbConfig["store_path"].(string), chainId, "store_result", "LOCK")))
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.TxExistDbConfig.LevelDbConfig["store_path"].(string), chainId, "store_txexist", "LOCK")))
	assert.Nil(t, os.RemoveAll(filepath.Join(ldbConfig.StorePath, chainId, "localdb", "LOCK")))

	//check recover result
	s, err = factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	assert.Nil(t, err)
	blockWithRWSets, err := s.GetBlockWithRWSets(5)
	assert.Nil(t, err)
	assert.Equal(t, block5.String(), blockWithRWSets.Block.String())
	for i := 0; i < len(blockWithRWSets.TxRWSets); i++ {
		assert.Equal(t, blockWithRWSets.TxRWSets[i].String(), blockWithRWSets.TxRWSets[i].String())
	}
	assert.Nil(t, s.Close())
}

func TestWriteBinlog(t *testing.T) {
	storePath := filepath.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().Unix()), logPath)
	opts := blockfiledb.DefaultOptions
	opts.ReadTimeOut = 1100

	bfdbPath := filepath.Join(storePath, chainId, blockFilePath)
	bfdb, bfdbErr := blockfiledb.Open(bfdbPath, "", opts, &test.GoLogger{})
	if bfdbErr != nil {
		panic(fmt.Sprintf("open block file db failed, path:%s, error:%s", bfdbPath, bfdbErr))
	}
	_, _, _, err := bfdb.Write(1, []byte("100"))
	assert.Nil(t, err)
}

func Test_blockchainStoreImpl_GetBlockWithTimeout(t *testing.T) {
	var factory = getFactory()
	lvlConf := getlvldbConfig("")

	// get block not timeout for 1200 ms
	lvlConf.ReadBFDBTimeOut = 1200 // 10 ms
	s, err := factory.NewStore(chainId, lvlConf, &test.GoLogger{}, nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = s.Close() }()
	init5Blocks(s)
	blk, err := s.GetBlock(4)
	assert.Nil(t, err)
	assert.Equal(t, blk.Header.BlockHeight, uint64(4))
}

//
//func TestLeveldbRange(t *testing.T) {
//	db, err := leveldb.OpenFile("gossip.db", nil)
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	wo := &opt.WriteOptions{Sync: true}
//	db.Put([]byte("key-1a"), []byte("value-1"), wo)
//	db.Put([]byte("key-3c"), []byte("value-3"), wo)
//	db.Put([]byte("key-4d"), []byte("value-4"), wo)
//	db.Put([]byte("key-5eff"), []byte("value-5"), wo)
//	db.Put([]byte("key-2b"), []byte("value-2"), wo)
//	iter := db.NewIterator(&util.Range{Start: []byte("key-1a"), Limit: []byte("key-3d")}, nil)
//	for iter.Next() {
//		fmt.Println(string(iter.Key()), string(iter.Value()))
//	}
//	iter.Release()
//	err = iter.Error()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	defer db.Close()
//}

func Test_blockchainStoreImpl_Mysql_Archive(t *testing.T) {
	//var factory = getFactory()
	//s, err := factory.NewStore(chainId, getSqliteConfig(), &test.GoLogger{}, nil)
	//assert.Equal(t, nil, err)
	//defer func(s protocol.BlockchainStore) {
	//
	//	s.Close()
	//}(s)
	//
	//err = s.ArchiveBlock(0)
	//assert.Equal(t, nil, err)
	//
	//err = s.RestoreBlocks(nil)
	//assert.Equal(t, nil, err)
	//
	//archivedPivot := s.GetArchivedPivot()
	//assert.True(t, archivedPivot == 0)
}

func Test_blockchainStoreImpl_Archive(t *testing.T) {
	//var factory = getFactory()
	//dbConf := getlvldbConfig("")
	//dbConf.UnArchiveBlockHeight = 10
	//s, err := factory.NewStore(chainId, dbConf, log, nil)
	//assert.Equal(t, nil, err)
	//defer func(s protocol.BlockchainStore) {
	//
	//	s.Close()
	//}(s)
	//
	//totalHeight := 60
	//archiveHeight1 := 27
	//archiveHeight2 := 30
	//archiveHeight3 := 43
	//
	////Prepare block data
	//blocks := make([]*commonPb.Block, 0, totalHeight)
	//txRWSetMp := make(map[uint64][]*commonPb.TxRWSet)
	//for i := 0; i < totalHeight; i++ {
	//	var (
	//		block   *commonPb.Block
	//		txRWSet []*commonPb.TxRWSet
	//	)
	//
	//	if i%5 == 0 {
	//		block, txRWSet = createConfBlockAndRWSets(chainId, uint64(i))
	//	} else {
	//		block, txRWSet = createBlockAndRWSets(chainId, uint64(i), 10)
	//	}
	//
	//	err = s.PutBlock(block, txRWSet)
	//	assert.Equal(t, nil, err)
	//	blocks = append(blocks, block)
	//	txRWSetMp[block.Header.BlockHeight] = txRWSet
	//}
	//
	//verifyArchive(t, 0, blocks, s)
	//
	////archive block height1
	//err = s.ArchiveBlock(uint64(archiveHeight1))
	//assert.Equal(t, nil, err)
	//assert.Equal(t, uint64(archiveHeight1), s.GetArchivedPivot())
	//
	//verifyArchive(t, 10, blocks, s)
	//
	////archive block height2 which is a config block
	//err1 := s.ArchiveBlock(uint64(archiveHeight2))
	//assert.True(t, err1 == archive.ErrConfigBlockArchive)
	//assert.Equal(t, uint64(archiveHeight1), s.GetArchivedPivot())
	//
	//verifyArchive(t, 15, blocks, s)
	//
	////archive block height3
	//err = s.ArchiveBlock(uint64(archiveHeight3))
	//assert.Equal(t, nil, err)
	//assert.Equal(t, uint64(archiveHeight3), s.GetArchivedPivot())
	//
	//verifyArchive(t, 25, blocks, s)
	//
	////Prepare restore data
	//blocksBytes := make([][]byte, 0, archiveHeight3-archiveHeight2+1)
	//for i := archiveHeight2; i <= archiveHeight3; i++ {
	//	blockBytes, _, err5 := serialization.SerializeBlock(&storePb.BlockWithRWSet{
	//		Block:          blocks[i],
	//		TxRWSets:       txRWSetMp[blocks[i].Header.BlockHeight],
	//		ContractEvents: nil,
	//	})
	//
	//	assert.Equal(t, nil, err5)
	//	blocksBytes = append(blocksBytes, blockBytes)
	//}
	//
	////restore block
	//err = s.RestoreBlocks(blocksBytes)
	//assert.Equal(t, nil, err)
	//assert.Equal(t, uint64(archiveHeight2-1), s.GetArchivedPivot())
	//
	//verifyArchive(t, 10, blocks, s)
	//
	////wait kvdb compactrange
	//time.Sleep(5 * time.Second)
}

func verifyArchive(t *testing.T, confHeight uint64, blocks []*commonPb.Block, s protocol.BlockchainStore) {
	archivedPivot := s.GetArchivedPivot()

	if archivedPivot == 0 {
		verifyUnarchivedHeight(t, archivedPivot, blocks, s)
		verifyUnarchivedHeight(t, archivedPivot+1, blocks, s)
		return
	}

	//verify store apis: archived height
	verifyArchivedHeight(t, archivedPivot-1, blocks, s)

	//verify store apis: archivedPivot height
	verifyArchivedHeight(t, archivedPivot, blocks, s)

	//verify store apis: conf block height
	verifyUnarchivedHeight(t, confHeight, blocks, s)

	//verify store apis: unarchived height
	verifyUnarchivedHeight(t, archivedPivot+1, blocks, s)
}

func verifyUnarchivedHeight(t *testing.T, avBlkHeight uint64, blocks []*commonPb.Block, s protocol.BlockchainStore) {
	avBlk := blocks[avBlkHeight]
	vbHeight, err1 := s.GetHeightByHash(avBlk.Header.BlockHash)
	assert.True(t, err1 == nil)
	assert.Equal(t, vbHeight, avBlkHeight)

	header, err2 := s.GetBlockHeaderByHeight(avBlk.Header.BlockHeight)
	assert.True(t, err2 == nil)
	assert.True(t, bytes.Equal(header.BlockHash, avBlk.Header.BlockHash))

	vtHeight, err4 := s.GetTxHeight(avBlk.Txs[0].Payload.TxId)
	assert.True(t, err4 == nil)
	assert.Equal(t, vtHeight, avBlkHeight)

	vtBlk, err5 := s.GetBlockByTx(avBlk.Txs[0].Payload.TxId)
	assert.True(t, err5 == nil)
	assert.Equal(t, avBlk.Header.ChainId, vtBlk.Header.ChainId)

	vttx, err6 := s.GetTx(avBlk.Txs[0].Payload.TxId)
	assert.True(t, err6 == nil)
	assert.Equal(t, avBlk.Header.ChainId, vttx.Payload.ChainId)

	vtBlk2, err7 := s.GetBlockByHash(avBlk.Hash())
	assert.True(t, err7 == nil)
	assert.Equal(t, avBlk.Header.ChainId, vtBlk2.Header.ChainId)

	vtBlkRW, err8 := s.GetBlockWithRWSets(avBlk.Header.BlockHeight)
	assert.True(t, err8 == nil)
	assert.Equal(t, avBlk.Header.ChainId, vtBlkRW.Block.Header.ChainId)

	vtBlkRWs, err9 := s.GetTxRWSetsByHeight(avBlk.Header.BlockHeight)
	assert.True(t, err9 == nil)
	assert.Equal(t, len(avBlk.Txs), len(vtBlkRWs))
	if len(avBlk.Txs) > 0 {
		assert.Equal(t, avBlk.Txs[0].Payload.TxId, vtBlkRWs[0].TxId)
	}
}

func verifyArchivedHeight(t *testing.T, avBlkHeight uint64, blocks []*commonPb.Block, s protocol.BlockchainStore) {
	avBlk := blocks[avBlkHeight]
	vbHeight, err1 := s.GetHeightByHash(avBlk.Header.BlockHash)
	assert.True(t, err1 == nil)
	assert.Equal(t, vbHeight, avBlkHeight)

	header, err2 := s.GetBlockHeaderByHeight(avBlk.Header.BlockHeight)
	assert.True(t, err2 == nil)
	assert.True(t, bytes.Equal(header.BlockHash, avBlk.Header.BlockHash))

	vtHeight, err4 := s.GetTxHeight(avBlk.Txs[0].Payload.TxId)
	assert.True(t, err4 == nil)
	assert.Equal(t, vtHeight, avBlkHeight)

	vtBlk, err5 := s.GetBlockByTx(avBlk.Txs[0].Payload.TxId)
	assert.True(t, archive.ErrArchivedBlock == err5)
	assert.True(t, vtBlk == nil)

	vttx, err6 := s.GetTx(avBlk.Txs[0].Payload.TxId)
	assert.True(t, archive.ErrArchivedTx == err6)
	assert.True(t, vttx == nil)

	vtBlk2, err7 := s.GetBlockByHash(avBlk.Hash())
	assert.True(t, archive.ErrArchivedBlock == err7)
	assert.True(t, vtBlk2 == nil)

	vtBlkRW, err8 := s.GetBlockWithRWSets(avBlk.Header.BlockHeight)
	assert.True(t, archive.ErrArchivedBlock == err8)
	assert.True(t, vtBlkRW == nil)

	vtBlkRWs, err9 := s.GetTxRWSetsByHeight(avBlk.Header.BlockHeight)
	assert.True(t, archive.ErrArchivedRWSet == err9)
	assert.True(t, vtBlkRWs == nil)
}

func TestBlockStoreImpl_GetArchivedPivot(t *testing.T) {
	var factory = getFactory()
	ldbConfig := getlvldbConfig("")
	ldbConfig.DisableBlockFileDb = false
	s, err := factory.NewStore(chainId, ldbConfig, &test.GoLogger{}, nil)
	//defer s.Close()
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)

	assert.Equal(t, true, ok)
	init5Blocks(s)

	height := bs.GetArchivedPivot()
	assert.Equal(t, uint64(0), height)

	sqldbConfig := getSqliteConfig()
	s, err = factory.NewStore(chainId, sqldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	bs, ok = s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)
	height = bs.GetArchivedPivot()
	assert.Equal(t, uint64(0), height)
}

func TestBlockStoreImpl_GetBlockHeaderByHeight(t *testing.T) {
	var factory = getFactory()
	sqldbConfig := getSqliteConfig()
	s, err := factory.NewStore(chainId, sqldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)
	header, err := bs.GetBlockHeaderByHeight(1)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), header.BlockHeight)
	header, err = bs.GetBlockHeaderByHeight(10)
	assert.Nil(t, header)
	assert.Nil(t, err)
}

func TestBlockStoreImpl_GetLastConfigBlock(t *testing.T) {
	var factory = getFactory()
	sqldbConfig := getSqliteConfig()
	s, err := factory.NewStore(chainId, sqldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)
	lastConfigBlock, err := bs.GetLastConfigBlock()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), lastConfigBlock.Header.BlockHeight)
}

func TestBlockStoreImpl_GetLastChainConfig(t *testing.T) {
	var factory = getFactory()
	sqldbConfig := getSqliteConfig()
	s, err := factory.NewStore(chainId, sqldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	init5Blocks(s)
	chainConfig, err := bs.GetLastChainConfig()
	assert.Nil(t, err)
	assert.Equal(t, chainConfig.ChainId, "")
}

func TestBlockStoreImpl_GetTxHeight(t *testing.T) {
	var factory = getFactory()
	lvldbConfig := getlvldbConfig("")
	lvldbConfig.DisableHistoryDB = true
	s, err := factory.NewStore(chainId, lvldbConfig, &test.GoLogger{}, nil)
	assert.Equal(t, nil, err)
	bs, ok := s.(*BlockStoreImpl)
	assert.Equal(t, true, ok)
	//init5Blocks(s)
	genesis := &storePb.BlockWithRWSet{Block: block0, TxRWSets: getTxRWSets()}
	err = s.InitGenesis(genesis)
	assert.Nil(t, err)
	txRWSets := getTxRWSets()
	txRWSets[0].TxId = block1.Txs[0].Payload.TxId
	err = s.PutBlock(block1, txRWSets)
	assert.Nil(t, err)
	height, err := bs.GetTxHeight(txRWSets[0].TxId)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), height)
}

func TestSlowLog(t *testing.T) {
	ctl := gomock.NewController(t)
	sdb := statedb.NewMockStateDB(ctl)
	sdb.EXPECT().ReadObject(gomock.Any(), gomock.Any()).DoAndReturn(func(string, []byte) ([]byte, error) {
		time.Sleep(150 * time.Millisecond)
		return []byte("value"), nil
	})
	log := mock.NewMockLogger(ctl)
	str := ""
	log.EXPECT().Debugf(gomock.Any(), gomock.Any()).DoAndReturn(func(f string, arg ...interface{}) {
		str += "DEBUG:" + fmt.Sprintf(f, arg...)
	})
	log.EXPECT().Infof(gomock.Any(), gomock.Any()).DoAndReturn(func(f string, arg ...interface{}) {
		str += "INFO:" + fmt.Sprintf(f, arg...)
	})
	log.EXPECT().Info(gomock.Any()).DoAndReturn(func(arg ...interface{}) {
		str += "INFO:" + fmt.Sprintf("%v", arg...)
	})

	bdb := blockdb.NewMockBlockDB(ctl)
	bdb.EXPECT().GetLastSavepoint().Return(uint64(0), nil)

	s, err := NewBlockStoreImpl("c1", &conf.StorageConfig{SlowLog: 100}, bdb, sdb, nil,
		nil, nil, nil, nil, log, binlog.NewMemBinlog(log), nil, nil, nil)
	assert.Equal(t, nil, err)
	v, err := s.ReadObject("a", []byte("b"))
	assert.NoError(t, err)
	t.Log(string(v))
	t.Log(str)
	assert.Contains(t, str, "slow log")
}
