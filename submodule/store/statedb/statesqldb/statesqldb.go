/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package statesqldb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	configPb "chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/store/v2/serialization"
	"chainmaker.org/chainmaker/store/v2/types"
	"chainmaker.org/chainmaker/utils/v2"
)

// StateSqlDB provider a implementation of `statedb.StateDB`
// @Description:
// This implementation provides a mysql based data model
type StateSqlDB struct {
	sysDbHandle protocol.SqlDBHandle
	contractDbs *types.ConnectionPoolDBHandle
	newDbHandle NewDbHandleFunc
	logger      protocol.Logger
	chainId     string
	sync.Mutex
	dbPrefix string
}

// getSysDbName 返回database的名字，为了解决多链不重复而使用
// @Description:
// @param dbPrefix
// @param chainId
// @return string
func getSysDbName(dbPrefix, chainId string) string {
	return dbPrefix + "statedb_" + chainId
}

// getContractDbName calculate contract db name, if name length>64, keep start 50 chars add 10 hash chars and 4 tail
// @Description:
// @param dbPrefix
// @param chainId
// @param contractName
// @return string
func getContractDbName(dbPrefix, chainId, contractName string) string {
	if _, ok := syscontract.SystemContract_value[contractName]; ok || len(contractName) == 0 {
		//如果是系统合约，不为每个合约构建数据库，使用统一个statedb数据库,合约名为空也使用系统合约数据库
		return getSysDbName(dbPrefix, chainId)
	}
	dbName := dbPrefix + "statedb_" + chainId + "_" + contractName
	if len(dbName) > 64 { //for mysql only support 64 chars
		h := sha256.New()
		h.Write([]byte(dbName))
		sum := h.Sum(nil)
		dbName = dbName[:50] + hex.EncodeToString(sum)[:10] + contractName[len(contractName)-4:]
	}
	return dbName
}

// initContractDb 如果数据库不存在，则创建数据库，然后切换到这个数据库，创建表
// @Description:
// 如果数据库存在，则切换数据库，检查表是否存在，不存在则创建表。
// @receiver db
// @param contractName
// @return error
func (db *StateSqlDB) initContractDb(contractName string) error {
	dbName := getContractDbName(db.dbPrefix, db.chainId, contractName)
	db.logger.Debugf("try to create state db %s", dbName)
	//为新合约创建对应的数据库
	_, err := db.sysDbHandle.CreateDatabaseIfNotExist(dbName)
	if err != nil {
		db.logger.Panic("init state sql db fail")
	}
	db.logger.Debugf("try to create state db table: state_infos for contract[%s]", contractName)
	//获得新合约对应数据库的DBHandle
	dbHandle := db.getContractDbHandle(contractName)
	//为新合约数据库初始化表
	err = dbHandle.CreateTableIfNotExist(&StateInfo{})
	if err != nil {
		db.logger.Panic("init state info sql db table fail:" + err.Error())
	}
	// 表不存在，则创建
	err = dbHandle.CreateTableIfNotExist(&StateRecordSql{})
	if err != nil {
		db.logger.Panic("init state record sql sql db table fail:" + err.Error())
	}
	return nil
}

// initSystemStateDb
// @Description: 初始化系统默认使用的状态数据库
// @receiver db
// @return error
func (db *StateSqlDB) initSystemStateDb() error {
	dbName := getSysDbName(db.dbPrefix, db.chainId)
	db.logger.Debugf("try to create state db %s", dbName)
	_, err := db.sysDbHandle.CreateDatabaseIfNotExist(dbName)
	if err != nil {
		db.logger.Panic("init state sql db fail")
	}
	db.logger.Debug("try to create system state db table: state_infos")
	err = db.sysDbHandle.CreateTableIfNotExist(&StateInfo{})
	if err != nil {
		db.logger.Panic("init state sql db table fail:" + err.Error())
	}
	err = db.sysDbHandle.CreateTableIfNotExist(&MemberExtraInfo{})
	if err != nil {
		db.logger.Panic("init member extra info table fail:" + err.Error())
	}
	db.logger.Debug("try to create system state db table: save_points")
	err = db.sysDbHandle.CreateTableIfNotExist(&types.SavePoint{})
	if err != nil {
		db.logger.Panic("init state sql db table fail:" + err.Error())
	}

	_, err = db.sysDbHandle.Save(&types.SavePoint{BlockHeight: 0})
	return err
}

// NewStateSqlDB construct a new `StateDB` for given chainId
//func NewStateSqlDB(chainId string, dbConfig *localconf.SqlDbConfig, logger protocol.Logger) (*StateSqlDB, error) {
//	dbName := getDbName("dbConfig", chainId)
//	db := rawsqlprovider.NewSqlDBHandle(dbName, dbConfig, logger)
//	return newStateSqlDB(dbName, chainId, db, dbConfig, logger)
//}

// nolint
type NewDbHandleFunc func(dbName string) (protocol.SqlDBHandle, error)

// NewStateSqlDB construct StateSqlDB
// @Description:
// @param dbPrefix
// @param chainId
// @param db
// @param newDbHandle
// @param logger
// @param sqlDbConnPoolSize
// @return *StateSqlDB
// @return error
func NewStateSqlDB(dbPrefix, chainId string, db protocol.SqlDBHandle, newDbHandle NewDbHandleFunc,
	logger protocol.Logger, sqlDbConnPoolSize int) (*StateSqlDB, error) {
	stateDB := &StateSqlDB{
		sysDbHandle: db,
		newDbHandle: newDbHandle,
		logger:      logger,
		chainId:     chainId,
		dbPrefix:    dbPrefix,
		contractDbs: types.NewConnectionPoolDBHandle(sqlDbConnPoolSize, logger),
	}
	logger.Debugf("NewStateSqlDB with db handle[%p]", db)
	return stateDB, nil
}

// InitGenesis 初始化创世块写入
// @Description:
// @receiver s
// @param genesisBlock
// @return error
func (s *StateSqlDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	s.Lock()
	defer s.Unlock()
	//初始化状态数据库，建库和建表
	err := s.initSystemStateDb()
	if err != nil {
		return err
	}
	return s.commitBlock(genesisBlock, false)
}

// CommitBlock commits the state in an atomic operation
// @Description:
// @receiver s
// @param blockWithRWSet
// @param isCache
// @return error
func (s *StateSqlDB) CommitBlock(blockWithRWSet *serialization.BlockWithSerializedInfo, isCache bool) error {
	s.Lock()
	defer s.Unlock()
	return s.commitBlock(blockWithRWSet, false)
}

// commitBlock 提交区块和读写集
// @Description:
// @receiver s
// @param blockWithRWSet
// @param isCache
// @return error
func (s *StateSqlDB) commitBlock(blockWithRWSet *serialization.BlockWithSerializedInfo, isCache bool) error {
	block := blockWithRWSet.Block
	txRWSets := blockWithRWSet.TxRWSets
	txKey := block.GetTxKey()
	if len(txRWSets) == 0 && len(block.Header.ConsensusArgs) == 0 {
		s.logger.Warnf("block[%d] don't have any read write set data", block.Header.BlockHeight)
	}
	//1. get a db transaction
	dbTx, err := s.sysDbHandle.GetDbTransaction(txKey)
	s.logger.Infof("GetDbTransaction db:%v,err:%s", dbTx, err)
	processStateDbSqlOutside := false
	if err == nil { //外部已经开启了事务，不用重复创建事务
		s.logger.Debugf("db transaction[%s] already created outside, don't need process statedb sql again", txKey)
		processStateDbSqlOutside = true
	}
	//没有在外部开启事务，则开启事务，进行数据写入
	if !processStateDbSqlOutside {
		dbTx, err = s.sysDbHandle.BeginDbTransaction(txKey)
		if err != nil {
			return err
		}
	}

	//2. 如果是新建合约，则创建对应的数据库，并执行DDL
	if utils.IsContractMgmtBlock(block) {
		//创建对应合约的数据库
		payload := block.Txs[0].Payload
		runtime := payload.GetParameter(syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String())
		contractId := &commonPb.Contract{
			Name:        string(payload.GetParameter(syscontract.InitContract_CONTRACT_NAME.String())),
			Version:     string(payload.GetParameter(syscontract.InitContract_CONTRACT_VERSION.String())),
			RuntimeType: commonPb.RuntimeType(commonPb.RuntimeType_value[string(runtime)]),
		}
		//if contractId.RuntimeType == commonPb.RuntimeType_EVM {
		//	address, _ := evmutils.MakeAddressFromString(contractId.Name)
		//	contractId.Name = address.String()
		//}
		err = s.updateStateForContractInit(dbTx, block, contractId, txRWSets[0].TxWrites, processStateDbSqlOutside)
		if err != nil {
			err2 := s.sysDbHandle.RollbackDbTransaction(txKey)
			if err2 != nil {
				return err2
			}
			return err
		}
	} else {
		//3. 不是新建合约，是普通的合约调用，则在事务中更新数据
		for _, txRWSet := range txRWSets {
			for _, txWrite := range txRWSet.TxWrites {
				err = s.operateDbByWriteSet(dbTx, block, txWrite, processStateDbSqlOutside)
				if err != nil {
					err2 := s.sysDbHandle.RollbackDbTransaction(txKey)
					if err2 != nil {
						return err2
					}
					return err
				}
			}
		}
		//3.5 处理BlockHeader中ConsensusArgs对应的合约状态数据更新
		if len(block.Header.ConsensusArgs) > 0 {
			err = s.updateConsensusArgs(dbTx, block)
			if err != nil {
				err2 := s.sysDbHandle.RollbackDbTransaction(txKey)
				if err2 != nil {
					return err2
				}
				return err
			}
		}
	}
	//4. 更新MemberExtra
	err = s.updateBlockMemberExtra(dbTx, block)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	//5. 更新SavePoint
	err = s.updateSavePoint(dbTx, block.Header.BlockHeight)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	//6. commit transaction
	err = s.sysDbHandle.CommitDbTransaction(txKey)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	s.logger.Debugf("chain[%s]: commit block[%d] sql statedb", block.Header.ChainId, block.Header.BlockHeight)
	return nil
}

// updateBlockMemberExtra 更新block member 信息
// @Description:
// @receiver s
// @param dbTx
// @param block
// @return error
func (s *StateSqlDB) updateBlockMemberExtra(dbTx protocol.SqlDBTransaction, block *commonPb.Block) error {
	for _, tx := range block.Txs {
		if tx.Payload.Sequence > 0 && tx.Sender != nil {
			err := s.saveMemberExtraData(dbTx, tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// operateDbByWriteSet 更新 合约及合约key/value数据
// @Description:
// @receiver s
// @param dbTx
// @param block
// @param txWrite
// @param processStateDbSqlOutside
// @return error
func (s *StateSqlDB) operateDbByWriteSet(dbTx protocol.SqlDBTransaction,
	block *commonPb.Block, txWrite *commonPb.TxWrite, processStateDbSqlOutside bool) error {
	contractDbName := getContractDbName(s.dbPrefix, s.chainId, txWrite.ContractName)
	if txWrite.ContractName != "" { //切换DB
		err := dbTx.ChangeContextDb(contractDbName)
		if err != nil {
			return err
		}
	}
	if strings.Contains(string(txWrite.Key), "#sql#") { // 是sql
		// 没有在外面处理过，则在这里进行处理
		if !processStateDbSqlOutside {
			sql := string(txWrite.Value)
			if _, err := dbTx.ExecSql(sql); err != nil {
				s.logger.Errorf("execute sql[%s] get error:%s", txWrite.Value, err.Error())
				return err
			}
		}
	} else {
		stateInfo := NewStateInfo(txWrite.ContractName, txWrite.Key, txWrite.Value,
			uint64(block.Header.BlockHeight), block.GetTimestamp())
		if _, err := dbTx.Save(stateInfo); err != nil {
			s.logger.Errorf("save state key[%s] get error:%s", txWrite.Key, err.Error())
			return err
		}
	}
	return nil
}

// updateSavePoint 保存断点
// @Description:
// @receiver s
// @param dbTx
// @param height
// @return error
func (s *StateSqlDB) updateSavePoint(dbTx protocol.SqlDBTransaction, height uint64) error {
	sysdb := getSysDbName(s.dbPrefix, s.chainId)
	err := dbTx.ChangeContextDb(sysdb)
	if err != nil {
		return err
	}
	_, err = dbTx.ExecSql("update save_points set block_height=?", height)
	if err != nil {
		s.logger.Errorf("update save point to %d get an error:%s", height, err)
		return err
	}
	return nil
}

// updateStateForContractInit 如果是创建或者升级合约，那么需要创建对应的数据库和state_infos表，然后执行DDL语句，然后如果是KV数据，保存数据
// @Description:
// @receiver s
// @param dbTx
// @param block
// @param contractId
// @param writes
// @param processStateDbSqlOutside
// @return error
func (s *StateSqlDB) updateStateForContractInit(dbTx protocol.SqlDBTransaction, block *commonPb.Block,
	contractId *commonPb.Contract, writes []*commonPb.TxWrite, processStateDbSqlOutside bool) error {

	dbName := getContractDbName(s.dbPrefix, block.Header.ChainId, contractId.Name)
	s.logger.Debugf("start init new db:%s for contract[%s]", dbName, contractId.Name)
	err := s.initContractDb(contractId.Name) //创建合约的数据库和KV表
	if err != nil {
		return err
	}
	s.logger.DebugDynamic(func() string {
		str := "WriteSet:"
		for i, w := range writes {
			str += fmt.Sprintf("id:%d,contract:%s,key:%s,value len:%d;", i, w.ContractName, w.Key, len(w.Value))
		}
		return str
	})

	for _, txWrite := range writes {
		if strings.Contains(string(txWrite.Key), "#sql#") {
			// 是sql
			// 已经在VM执行的时候执行了SQL则不处理，只有快速同步的时候，没有经过VM执行，才需要直接把写集的SQL运行
			if !processStateDbSqlOutside {
				writeDbName := getContractDbName(s.dbPrefix, block.Header.ChainId, txWrite.ContractName)
				err = dbTx.ChangeContextDb(writeDbName)
				if err != nil {
					s.logger.Errorf("change context db to %s get an error:%s", writeDbName, err)
					return err
				}
				_, err = dbTx.ExecSql(string(txWrite.Value)) //运行用户自定义的建表语句
				if err != nil {
					s.logger.Errorf("execute sql[%s] get an error:%s", string(txWrite.Value), err)
					return err
				}
			}
		} else {
			//是KV数据，直接存储到StateInfo表
			stateInfo := NewStateInfo(txWrite.ContractName, txWrite.Key, txWrite.Value,
				uint64(block.Header.BlockHeight), block.GetTimestamp())
			writeDbName := getContractDbName(s.dbPrefix, block.Header.ChainId, txWrite.ContractName)
			err = dbTx.ChangeContextDb(writeDbName)
			if err != nil {
				return err
			}
			s.logger.Debugf("try save state key[%s] to db[%s]", txWrite.Key, writeDbName)
			if err = saveStateInfo(dbTx, stateInfo); err != nil {
				s.logger.Errorf("save state key[%s] to db[%s] get error:%s", txWrite.Key, writeDbName, err.Error())
				return err
			}
		}
	}
	s.logger.Debugf("chain[%s]: save state block[%d]",
		block.Header.ChainId, block.Header.BlockHeight)
	return nil
}

// saveStateInfo 保存state信息
// @Description:
// @param tx
// @param stateInfo
// @return error
func saveStateInfo(tx protocol.SqlDBTransaction, stateInfo *StateInfo) error {
	//if len(stateInfo.ObjectKey) == 0 {
	//	return errors.New("state key is empty")
	//}
	updateSql := `update state_infos set object_value=?,block_height=?
where contract_name=? and object_key=?`
	result, err := tx.ExecSql(updateSql, stateInfo.ObjectValue, stateInfo.BlockHeight, stateInfo.ContractName,
		stateInfo.ObjectKey)
	if result == 0 || err != nil {
		insertSql := `INSERT INTO state_infos (contract_name,object_key,object_value,block_height)
VALUES (?,?,?,?)`
		_, err = tx.ExecSql(insertSql, stateInfo.ContractName, stateInfo.ObjectKey, stateInfo.ObjectValue,
			stateInfo.BlockHeight)
	}
	return err
}

// ReadObject returns the state value for given contract name and key, or returns nil if none exists.
// @Description:
// @receiver s
// @param contractName
// @param key
// @return []byte
// @return error
func (s *StateSqlDB) ReadObject(contractName string, key []byte) ([]byte, error) {
	s.Lock()
	defer s.Unlock()
	db := s.getContractDbHandle(contractName)
	sql := "select object_value from state_infos where contract_name=? and object_key=?"
	res, err := db.QuerySingle(sql, contractName, key)
	if err != nil {
		s.logger.Errorf("failed to read state, contract:%s, key:%s,error:%s", contractName, key, err)
		return nil, err
	}
	if res.IsEmpty() {
		s.logger.Debugf("db[%p] read empty state, contract:%s, key:%s", db, contractName, key)
		return nil, nil
	}
	var stateValue []byte

	err = res.ScanColumns(&stateValue)
	if err != nil {
		s.logger.Errorf("failed to read state, contract:%s, key:%s", contractName, key)
		return nil, err
	}
	s.logger.Debugf("read right state, contract:%s, key:%s valLen:%d", contractName, key, len(stateValue))
	return stateValue, nil
}

// ReadObjects returns the state values for given contract name and keys
func (s *StateSqlDB) ReadObjects(contractName string, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.Lock()
	defer s.Unlock()

	keysArr := []interface{}{}

	keysArr = append(keysArr, contractName)

	for i := 0; i < len(keys); i++ {
		if keys[i] == nil {
			continue
		}
		keysArr = append(keysArr, keys[i])
	}
	// get subArgs:?,?,?
	subArgs := ""
	for i := 0; i < len(keysArr)-1; i++ {
		if i == 0 {
			subArgs = subArgs + "?"
		} else {
			subArgs = subArgs + ",?"
		}
	}
	db := s.getContractDbHandle(contractName)
	// select object_value from state_infos where contract_name=? and object_key in (?,?,?)
	sql := "select object_key,object_value from state_infos where contract_name=? and object_key in (" + subArgs + ")"
	// keysArr:contractName,object_key1,object_key2...
	rows, err := db.QueryMulti(sql, keysArr...)
	if err != nil {
		s.logger.Errorf("failed to readObjects, contract:%s,error:%s", contractName, err)
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}

	dataMap := make(map[string][]byte)
	type KeyValue struct {
		ObjectKey   []byte `gorm:"size:128;primaryKey;default:''"`
		ObjectValue []byte `gorm:"type:longblob"`
	}
	for rows.Next() {
		kv := &KeyValue{}
		err = rows.ScanColumns(&kv.ObjectKey, &kv.ObjectValue)
		if err != nil {
			s.logger.Errorf("failed to readObjects,scan columns error, contract:%s,error:%s", contractName, err)
			return nil, err
		}
		dataMap[string(kv.ObjectKey)] = kv.ObjectValue
	}

	res := [][]byte{}
	for i := 0; i < len(keys); i++ {
		if keys[i] == nil {
			res = append(res, nil)
			continue
		}
		v, exist := dataMap[string(keys[i])]
		//not exsit
		if !exist {
			res = append(res, nil)
			continue
		}
		//exist
		copyData := make([]byte, len(v))
		copy(copyData, v)
		res = append(res, copyData)
	}

	return res, nil
}

// SelectObject returns an iterator that contains all the key-values between given key ranges.
// startKey is included in the results and limit is excluded.
// @Description:
// @receiver s
// @param contractName
// @param startKey
// @param limit means endKey
// @return protocol.StateIterator
// @return error
func (s *StateSqlDB) SelectObject(contractName string, startKey []byte, limit []byte) (protocol.StateIterator, error) {
	s.Lock()
	defer s.Unlock()
	db := s.getContractDbHandle(contractName)
	var rows protocol.SqlRows
	var err error
	if db.GetSqlDbType() == DbType_Sqlite { //sqlite是单数据库，所以所有stateInfo都在同一个表里，需要通过contractName进行区分
		sql := "select * from state_infos where contract_name=? and object_key >= ? and object_key  < ?"
		rows, err = db.QueryMulti(sql, contractName, startKey, limit)
	} else { //mysql 是一个合约一个数据库，所以当前连接中的contractName肯定是同一个，不需要再加contract_name=字段
		sql := "select * from state_infos where object_key >= ? and object_key  < ?"
		rows, err = db.QueryMulti(sql, startKey, limit)
	}
	if err != nil {
		return nil, err
	}
	return newKVIterator(rows), nil
}

// GetLastSavepoint returns the last block height
// @Description:
// @receiver s
// @return uint64
// @return error
func (s *StateSqlDB) GetLastSavepoint() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	sql := "select block_height from save_points"
	row, err := s.sysDbHandle.QuerySingle(sql)
	if err != nil {
		return 0, err
	}
	var height *uint64
	err = row.ScanColumns(&height)
	if err != nil {
		return 0, err
	}
	if height == nil {
		return 0, nil
	}
	return *height, nil
}

// isSystemContract 判断是否是系统合约
// @Description:
// @param name
// @return bool
func isSystemContract(name string) bool {
	_, ok := syscontract.SystemContract_value[name]
	return ok
}

// getContractDbHandle 获得合约对应db
// @Description:
// @receiver s
// @param contractName
// @return protocol.SqlDBHandle
func (s *StateSqlDB) getContractDbHandle(contractName string) protocol.SqlDBHandle {
	//系统合约共用系统默认的DB Handle
	if isSystemContract(contractName) {
		return s.sysDbHandle
	}
	if handle, ok := s.contractDbs.GetDBHandle(contractName); ok {
		s.logger.Debugf("reuse exist db handle for contract[%s],handle:%p", contractName, handle)
		return handle
	}

	//if s.dbConfig.SqlDbType == "sqlite" { //sqlite is a file db, don't create multi connection.
	//	s.contractDbs[contractName] = s.db
	//	return s.db
	//}
	dbName := getContractDbName(s.dbPrefix, s.chainId, contractName)
	//db := rawsqlprovider.NewSqlDBHandle(dbName, s.dbConfig, s.logger)
	if s.newDbHandle == nil {
		s.logger.Error("StateSqlDB.newDbHandle is null, use default db handle")
		return s.sysDbHandle
	}
	db, err := s.newDbHandle(dbName)
	if err != nil {
		s.logger.Error(err)
	}
	s.contractDbs.SetDBHandle(contractName, db)
	s.logger.Infof("create new sql db handle[%p] database[%s] for contract[%s]", db, dbName, contractName)
	return db
}

// Close is used to close database, there is no need for gorm to close db
// @Description:
// @receiver s
func (s *StateSqlDB) Close() {
	s.Lock()
	defer s.Unlock()
	s.logger.Info("close state sql db")
	s.sysDbHandle.Close()
	s.contractDbs.Clear()
	//for contract, db := range s.contractDbs {
	//	s.logger.Infof("close state sql db for contract:%s", contract)
	//	db.Close()
	//}
}

// QuerySingle 查询合约
// @Description:
// @receiver s
// @param contractName
// @param sql
// @param values
// @return protocol.SqlRow
// @return error
func (s *StateSqlDB) QuerySingle(contractName, sql string, values ...interface{}) (protocol.SqlRow, error) {
	s.Lock()
	defer s.Unlock()
	db := s.getContractDbHandle(contractName)

	row, err := db.QuerySingle(sql, values...)
	if err != nil {
		s.logger.Errorf("execute sql[%s] in statedb[%s] get an error:%s", sql, contractName, err)
		return nil, err
	}
	if row.IsEmpty() {
		s.logger.Infof("query single return empty row. sql:%s,db name:%s", sql, contractName)
	}
	return row, err
}

// QueryMulti 查询合约数据
// @Description:
// @receiver s
// @param contractName
// @param sql
// @param values
// @return protocol.SqlRows
// @return error
func (s *StateSqlDB) QueryMulti(contractName, sql string, values ...interface{}) (protocol.SqlRows, error) {
	s.Lock()
	defer s.Unlock()
	db := s.getContractDbHandle(contractName)

	return db.QueryMulti(sql, values...)

}

// ExecDdlSql 执行ddl语句 建库，建表，并写入 初始化数据
// @Description:
// @receiver s
// @param contractName
// @param sql
// @param version
// @return error
func (s *StateSqlDB) ExecDdlSql(contractName, sql, version string) error {
	s.Lock()
	defer s.Unlock()
	dbName := getContractDbName(s.dbPrefix, s.chainId, contractName)
	exist, err := s.sysDbHandle.CreateDatabaseIfNotExist(dbName)
	if err != nil {
		return err
	}
	if !exist {
		if errTmp := s.initContractDb(contractName); errTmp != nil {
			return errTmp
		}
	}
	db := s.getContractDbHandle(contractName)
	// query ddl from db
	record := NewStateRecordSql(contractName, sql, protocol.SqlTypeDdl, version, 0)
	query, args := record.GetQueryStatusSql()
	s.logger.Debug("Query sql:", query, args)
	row, _ := db.QuerySingle(query, args)
	//查询数据库中是否有DDL记录，如果有对应记录，而且状态是1，那么就跳过重复执行DDL的情况
	if row != nil && !row.IsEmpty() {
		status := 0
		err = row.ScanColumns(&status)
		if err != nil {
			return err
		}
		if status == 1 { //SUCCESS
			s.logger.Infof("DDLRecord[%s] already executed, ignore it", sql)
			return nil
		}
	}
	insertSql, args2 := record.GetInsertSql("")
	_, err = db.ExecSql(insertSql, args2...)
	if err != nil {
		s.logger.Warnf("DDLRecord[%s] save fail. error: %s", sql, err.Error())
	}
	//查询不到记录，或者查询出来后状态是失败，则执行DDL
	s.logger.Debugf("run DDL sql[%s] in db[%s]", sql, dbName)
	_, err = db.ExecSql(sql)

	record.Status = 1
	updateSql, args3 := record.GetUpdateSql()
	_, err2 := db.ExecSql(updateSql, args3...)
	if err2 != nil {
		s.logger.Warnf("DDLRecord[%s] update fail. error: %s", sql, err2.Error())
	}
	return err
}

// BeginDbTransaction 开启事务
// @Description:
// @receiver s
// @param txName
// @return protocol.SqlDBTransaction
// @return error
func (s *StateSqlDB) BeginDbTransaction(txName string) (protocol.SqlDBTransaction, error) {
	s.Lock()
	defer s.Unlock()
	return s.sysDbHandle.BeginDbTransaction(txName)
}

// GetDbTransaction 获得事务
// @Description:
// @receiver s
// @param txName
// @return protocol.SqlDBTransaction
// @return error
func (s *StateSqlDB) GetDbTransaction(txName string) (protocol.SqlDBTransaction, error) {
	s.Lock()
	defer s.Unlock()
	return s.sysDbHandle.GetDbTransaction(txName)

}

// CommitDbTransaction 提交事务
// @Description:
// @receiver s
// @param txName
// @return error
func (s *StateSqlDB) CommitDbTransaction(txName string) error {
	s.Lock()
	defer s.Unlock()
	return s.sysDbHandle.CommitDbTransaction(txName)

}

// RollbackDbTransaction 事务回滚
// @Description:
// @receiver s
// @param txName
// @return error
func (s *StateSqlDB) RollbackDbTransaction(txName string) error {
	s.Lock()
	defer s.Unlock()
	s.logger.Warnw("rollback db transaction:", txName)
	return s.sysDbHandle.RollbackDbTransaction(txName)
}

// CreateDatabase  创建 database
// @Description:
// @receiver s
// @param contractName
// @return error
func (s *StateSqlDB) CreateDatabase(contractName string) error {
	dbName := getContractDbName(s.dbPrefix, s.chainId, contractName)
	sql := "CREATE DATABASE " + dbName + " char set utf8"
	return s.ExecDdlSql("", sql, "")
}

// DropDatabase 删除一个合约对应的数据库
// @Description:
// @receiver s
// @param contractName
// @return error
func (s *StateSqlDB) DropDatabase(contractName string) error {
	dbName := getContractDbName(s.dbPrefix, s.chainId, contractName)
	sql := "DROP DATABASE " + dbName
	return s.ExecDdlSql("", sql, "")
}

// GetContractDbName 获得一个合约对应的状态数据库名
// @Description:
// @receiver s
// @param contractName
// @return string
func (s *StateSqlDB) GetContractDbName(contractName string) string {
	return getContractDbName(s.dbPrefix, s.chainId, contractName)
}

// updateConsensusArgs 更新共识信息
// @Description:
// @receiver s
// @param dbTx
// @param block
// @return error
func (s *StateSqlDB) updateConsensusArgs(dbTx protocol.SqlDBTransaction, block *commonPb.Block) error {
	//try to add consensusArgs
	consensusArgs, err := utils.GetConsensusArgsFromBlock(block)
	if err != nil {
		s.logger.Errorf("parse header.ConsensusArgs get an error:%s", err)
		return err
	}
	if consensusArgs.ConsensusData != nil {
		s.logger.Debugf("add consensusArgs ConsensusData to statedb")
		for _, write := range consensusArgs.ConsensusData.TxWrites {
			err = s.operateDbByWriteSet(dbTx, block, write, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// GetChainConfig 获得链的配置信息
// @Description:
// @receiver s
// @return *configPb.ChainConfig
// @return error
func (s *StateSqlDB) GetChainConfig() (*configPb.ChainConfig, error) {
	val, err := s.ReadObject(syscontract.SystemContract_CHAIN_CONFIG.String(),
		[]byte(syscontract.SystemContract_CHAIN_CONFIG.String()))
	if err != nil {
		return nil, err
	}
	conf := &configPb.ChainConfig{}
	err = conf.Unmarshal(val)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

// GetMemberExtraData 获得 member相关信息
// @Description:
// @receiver s
// @param member
// @return *accesscontrol.MemberExtraData
// @return error
func (s *StateSqlDB) GetMemberExtraData(member *accesscontrol.Member) (*accesscontrol.MemberExtraData, error) {
	s.Lock()
	defer s.Unlock()
	mei := &MemberExtraInfo{}
	sql := "select * from " + mei.GetTableName() + " where member_hash=?"
	row, err := s.sysDbHandle.QuerySingle(sql, getMemberHash(member))
	if err != nil {
		return nil, err
	}
	err = mei.ScanObject(row.ScanColumns)

	if err != nil {
		return nil, err
	}

	return mei.GetExtraData(), nil
}

// saveMemberExtraData 保存 member数据
// @Description:
// @receiver s
// @param dbTx
// @param tx
// @return error
func (s *StateSqlDB) saveMemberExtraData(dbTx protocol.SqlDBTransaction, tx *commonPb.Transaction) error {
	extraData := NewMemberExtraInfo(tx.Sender.Signer, &accesscontrol.MemberExtraData{Sequence: tx.Payload.Sequence})
	_, err := dbTx.Save(extraData)
	if err != nil {
		return err
	}
	return nil
}

// GetContractByName 返回合约信息
// @Description:
// @receiver s
// @param name
// @return *commonPb.Contract
// @return error
func (s *StateSqlDB) GetContractByName(name string) (*commonPb.Contract, error) {
	return utils.GetContractByName(s.ReadObject, name)
}

// GetContractBytecode 获得 合约对应字节码数据
// @Description:
// @receiver s
// @param name
// @return []byte
// @return error
func (s *StateSqlDB) GetContractBytecode(name string) ([]byte, error) {
	return utils.GetContractBytecode(s.ReadObject, name)
}
