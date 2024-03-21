/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package historykvdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"chainmaker.org/chainmaker/store/v2/historydb"
)

// constructContractTxHistKey construct contract tx history key
// key format : c{contractName}#{blockHeight}#{txId}
// @Description:
// @param contractName
// @param blockHeight
// @param txId
// @return []byte
func constructContractTxHistKey(contractName string, blockHeight uint64, txId string) []byte {
	key := fmt.Sprintf(contractTxHistoryPrefix+"%s"+splitChar+"%d"+splitChar+"%s", contractName, blockHeight, txId)
	return []byte(key)
}

// constructContractTxHistKeyPrefix construct contract tx history prefix
// key format : c{contractName}#
// @Description:
// @param contractName
// @return []byte
func constructContractTxHistKeyPrefix(contractName string) []byte {
	key := fmt.Sprintf(contractTxHistoryPrefix+"%s"+splitChar, contractName)
	return []byte(key)
}

// splitContractTxHistKey split contract tx history key
// @Description:
// @param key
// @return contractName
// @return blockHeight
// @return txId
// @return err
func splitContractTxHistKey(key []byte) (contractName string, blockHeight uint64, txId string, err error) {
	if len(key) == 0 {
		err = errors.New("empty dbKey")
		return
	}
	array := strings.Split(string(key[1:]), splitChar)
	if len(array) != 3 {
		err = errors.New("invalid dbKey format")
		return
	}
	contractName = array[0]
	height, err := strconv.Atoi(array[1])
	blockHeight = uint64(height)
	txId = array[2]
	return
}

// GetContractTxHistory construct an iterator , according to contractName
// @Description:
// @receiver h
// @param contractName
// @return historydb.HistoryIterator
// @return error
func (h *HistoryKvDB) GetContractTxHistory(contractName string) (historydb.HistoryIterator, error) {
	iter, erro := h.dbHandle.NewIteratorWithPrefix(constructContractTxHistKeyPrefix(contractName))
	if erro != nil {
		return nil, erro
	}
	splitKeyFunc := func(key []byte) (*historydb.BlockHeightTxId, error) {
		_, height, txId, err := splitContractTxHistKey(key)
		if err != nil {
			return nil, err
		}
		return &historydb.BlockHeightTxId{
			BlockHeight: height,
			TxId:        txId,
		}, nil
	}
	return &historyKeyIterator{dbIter: iter, buildFunc: splitKeyFunc}, nil
}
