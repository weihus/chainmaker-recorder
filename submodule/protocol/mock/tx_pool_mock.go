// Code generated by MockGen. DO NOT EDIT.
// Source: tx_pool_interface.go

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	common "chainmaker.org/chainmaker/pb-go/v2/common"
	txpool "chainmaker.org/chainmaker/pb-go/v2/txpool"
	protocol "chainmaker.org/chainmaker/protocol/v2"
	gomock "github.com/golang/mock/gomock"
)

// MockTxPool is a mock of TxPool interface.
type MockTxPool struct {
	ctrl     *gomock.Controller
	recorder *MockTxPoolMockRecorder
}

// MockTxPoolMockRecorder is the mock recorder for MockTxPool.
type MockTxPoolMockRecorder struct {
	mock *MockTxPool
}

// NewMockTxPool creates a new mock instance.
func NewMockTxPool(ctrl *gomock.Controller) *MockTxPool {
	mock := &MockTxPool{ctrl: ctrl}
	mock.recorder = &MockTxPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTxPool) EXPECT() *MockTxPoolMockRecorder {
	return m.recorder
}

// AddTx mocks base method.
func (m *MockTxPool) AddTx(tx *common.Transaction, source protocol.TxSource) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddTx", tx, source)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddTx indicates an expected call of AddTx.
func (mr *MockTxPoolMockRecorder) AddTx(tx, source interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTx", reflect.TypeOf((*MockTxPool)(nil).AddTx), tx, source)
}

// AddTxBatchesToPendingCache mocks base method.
func (m *MockTxPool) AddTxBatchesToPendingCache(batchIds []string, blockHeight uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddTxBatchesToPendingCache", batchIds, blockHeight)
}

// AddTxBatchesToPendingCache indicates an expected call of AddTxBatchesToPendingCache.
func (mr *MockTxPoolMockRecorder) AddTxBatchesToPendingCache(batchIds, blockHeight interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTxBatchesToPendingCache", reflect.TypeOf((*MockTxPool)(nil).AddTxBatchesToPendingCache), batchIds, blockHeight)
}

// AddTxsToPendingCache mocks base method.
func (m *MockTxPool) AddTxsToPendingCache(txs []*common.Transaction, blockHeight uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddTxsToPendingCache", txs, blockHeight)
}

// AddTxsToPendingCache indicates an expected call of AddTxsToPendingCache.
func (mr *MockTxPoolMockRecorder) AddTxsToPendingCache(txs, blockHeight interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTxsToPendingCache", reflect.TypeOf((*MockTxPool)(nil).AddTxsToPendingCache), txs, blockHeight)
}

// FetchTxBatches mocks base method.
func (m *MockTxPool) FetchTxBatches(blockHeight uint64) ([]string, [][]*common.Transaction) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchTxBatches", blockHeight)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].([][]*common.Transaction)
	return ret0, ret1
}

// FetchTxBatches indicates an expected call of FetchTxBatches.
func (mr *MockTxPoolMockRecorder) FetchTxBatches(blockHeight interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchTxBatches", reflect.TypeOf((*MockTxPool)(nil).FetchTxBatches), blockHeight)
}

// FetchTxs mocks base method.
func (m *MockTxPool) FetchTxs(blockHeight uint64) []*common.Transaction {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchTxs", blockHeight)
	ret0, _ := ret[0].([]*common.Transaction)
	return ret0
}

// FetchTxs indicates an expected call of FetchTxs.
func (mr *MockTxPoolMockRecorder) FetchTxs(blockHeight interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchTxs", reflect.TypeOf((*MockTxPool)(nil).FetchTxs), blockHeight)
}

// GetAllTxsByBatchIds mocks base method.
func (m *MockTxPool) GetAllTxsByBatchIds(batchIds []string, proposerId string, height uint64, timeoutMs int) ([][]*common.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllTxsByBatchIds", batchIds, proposerId, height, timeoutMs)
	ret0, _ := ret[0].([][]*common.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllTxsByBatchIds indicates an expected call of GetAllTxsByBatchIds.
func (mr *MockTxPoolMockRecorder) GetAllTxsByBatchIds(batchIds, proposerId, height, timeoutMs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllTxsByBatchIds", reflect.TypeOf((*MockTxPool)(nil).GetAllTxsByBatchIds), batchIds, proposerId, height, timeoutMs)
}

// GetAllTxsByTxIds mocks base method.
func (m *MockTxPool) GetAllTxsByTxIds(txIds []string, proposerId string, height uint64, timeoutMs int) (map[string]*common.Transaction, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllTxsByTxIds", txIds, proposerId, height, timeoutMs)
	ret0, _ := ret[0].(map[string]*common.Transaction)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllTxsByTxIds indicates an expected call of GetAllTxsByTxIds.
func (mr *MockTxPoolMockRecorder) GetAllTxsByTxIds(txIds, proposerId, height, timeoutMs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllTxsByTxIds", reflect.TypeOf((*MockTxPool)(nil).GetAllTxsByTxIds), txIds, proposerId, height, timeoutMs)
}

// GetPoolStatus mocks base method.
func (m *MockTxPool) GetPoolStatus() *txpool.TxPoolStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPoolStatus")
	ret0, _ := ret[0].(*txpool.TxPoolStatus)
	return ret0
}

// GetPoolStatus indicates an expected call of GetPoolStatus.
func (mr *MockTxPoolMockRecorder) GetPoolStatus() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPoolStatus", reflect.TypeOf((*MockTxPool)(nil).GetPoolStatus))
}

// GetTxIdsByTypeAndStage mocks base method.
func (m *MockTxPool) GetTxIdsByTypeAndStage(txType, txStage int32) []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTxIdsByTypeAndStage", txType, txStage)
	ret0, _ := ret[0].([]string)
	return ret0
}

// GetTxIdsByTypeAndStage indicates an expected call of GetTxIdsByTypeAndStage.
func (mr *MockTxPoolMockRecorder) GetTxIdsByTypeAndStage(txType, txStage interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTxIdsByTypeAndStage", reflect.TypeOf((*MockTxPool)(nil).GetTxIdsByTypeAndStage), txType, txStage)
}

// GetTxsByTxIds mocks base method.
func (m *MockTxPool) GetTxsByTxIds(txIds []string) (map[string]*common.Transaction, map[string]struct{}) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTxsByTxIds", txIds)
	ret0, _ := ret[0].(map[string]*common.Transaction)
	ret1, _ := ret[1].(map[string]struct{})
	return ret0, ret1
}

// GetTxsByTxIds indicates an expected call of GetTxsByTxIds.
func (mr *MockTxPoolMockRecorder) GetTxsByTxIds(txIds interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTxsByTxIds", reflect.TypeOf((*MockTxPool)(nil).GetTxsByTxIds), txIds)
}

// GetTxsInPoolByTxIds mocks base method.
func (m *MockTxPool) GetTxsInPoolByTxIds(txIds []string) ([]*common.Transaction, []string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTxsInPoolByTxIds", txIds)
	ret0, _ := ret[0].([]*common.Transaction)
	ret1, _ := ret[1].([]string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetTxsInPoolByTxIds indicates an expected call of GetTxsInPoolByTxIds.
func (mr *MockTxPoolMockRecorder) GetTxsInPoolByTxIds(txIds interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTxsInPoolByTxIds", reflect.TypeOf((*MockTxPool)(nil).GetTxsInPoolByTxIds), txIds)
}

// ReGenTxBatchesWithRemoveTxs mocks base method.
func (m *MockTxPool) ReGenTxBatchesWithRemoveTxs(blockHeight uint64, batchIds []string, removeTxs []*common.Transaction) ([]string, [][]*common.Transaction) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReGenTxBatchesWithRemoveTxs", blockHeight, batchIds, removeTxs)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].([][]*common.Transaction)
	return ret0, ret1
}

// ReGenTxBatchesWithRemoveTxs indicates an expected call of ReGenTxBatchesWithRemoveTxs.
func (mr *MockTxPoolMockRecorder) ReGenTxBatchesWithRemoveTxs(blockHeight, batchIds, removeTxs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReGenTxBatchesWithRemoveTxs", reflect.TypeOf((*MockTxPool)(nil).ReGenTxBatchesWithRemoveTxs), blockHeight, batchIds, removeTxs)
}

// ReGenTxBatchesWithRetryTxs mocks base method.
func (m *MockTxPool) ReGenTxBatchesWithRetryTxs(blockHeight uint64, batchIds []string, retryTxs []*common.Transaction) ([]string, [][]*common.Transaction) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReGenTxBatchesWithRetryTxs", blockHeight, batchIds, retryTxs)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].([][]*common.Transaction)
	return ret0, ret1
}

// ReGenTxBatchesWithRetryTxs indicates an expected call of ReGenTxBatchesWithRetryTxs.
func (mr *MockTxPoolMockRecorder) ReGenTxBatchesWithRetryTxs(blockHeight, batchIds, retryTxs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReGenTxBatchesWithRetryTxs", reflect.TypeOf((*MockTxPool)(nil).ReGenTxBatchesWithRetryTxs), blockHeight, batchIds, retryTxs)
}

// RemoveTxsInTxBatches mocks base method.
func (m *MockTxPool) RemoveTxsInTxBatches(batchIds []string, removeTxs []*common.Transaction) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemoveTxsInTxBatches", batchIds, removeTxs)
}

// RemoveTxsInTxBatches indicates an expected call of RemoveTxsInTxBatches.
func (mr *MockTxPoolMockRecorder) RemoveTxsInTxBatches(batchIds, removeTxs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveTxsInTxBatches", reflect.TypeOf((*MockTxPool)(nil).RemoveTxsInTxBatches), batchIds, removeTxs)
}

// RetryAndRemoveTxBatches mocks base method.
func (m *MockTxPool) RetryAndRemoveTxBatches(retryBatchIds, removeBatchIds []string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RetryAndRemoveTxBatches", retryBatchIds, removeBatchIds)
}

// RetryAndRemoveTxBatches indicates an expected call of RetryAndRemoveTxBatches.
func (mr *MockTxPoolMockRecorder) RetryAndRemoveTxBatches(retryBatchIds, removeBatchIds interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryAndRemoveTxBatches", reflect.TypeOf((*MockTxPool)(nil).RetryAndRemoveTxBatches), retryBatchIds, removeBatchIds)
}

// RetryAndRemoveTxs mocks base method.
func (m *MockTxPool) RetryAndRemoveTxs(retryTxs, removeTxs []*common.Transaction) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RetryAndRemoveTxs", retryTxs, removeTxs)
}

// RetryAndRemoveTxs indicates an expected call of RetryAndRemoveTxs.
func (mr *MockTxPoolMockRecorder) RetryAndRemoveTxs(retryTxs, removeTxs interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryAndRemoveTxs", reflect.TypeOf((*MockTxPool)(nil).RetryAndRemoveTxs), retryTxs, removeTxs)
}

// Start mocks base method.
func (m *MockTxPool) Start() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockTxPoolMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockTxPool)(nil).Start))
}

// Stop mocks base method.
func (m *MockTxPool) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockTxPoolMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockTxPool)(nil).Stop))
}

// TxExists mocks base method.
func (m *MockTxPool) TxExists(tx *common.Transaction) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TxExists", tx)
	ret0, _ := ret[0].(bool)
	return ret0
}

// TxExists indicates an expected call of TxExists.
func (mr *MockTxPoolMockRecorder) TxExists(tx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TxExists", reflect.TypeOf((*MockTxPool)(nil).TxExists), tx)
}