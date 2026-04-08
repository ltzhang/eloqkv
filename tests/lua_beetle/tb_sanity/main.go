// TigerBeetle Sanity Test
//
// Exercises the same 20 semantic cases as sanity_test.py against a live
// TigerBeetle instance using the official Go client.
//
// Usage:
//
//	go run . [--address <host:port>]
//
// Flags:
//
//	--address   TigerBeetle replica address  (default: "3000")
//
// Exit codes:
//
//	0  All tests passed
//	1  One or more tests failed
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// ============================================================================
// TigerBeetle error result codes (matching the Lua / Go constants)
// ============================================================================

const (
	errOK                           = 0
	errLinkedEventFailed            = 1
	errLinkedEventChainOpen         = 2
	errAccountsMustBeDifferent      = 12
	errDebitAccountNotFound         = 21
	errIDAlreadyExists              = 21
	errCreditAccountNotFound        = 22
	errAccountsMustHaveSameLedger   = 23
	errTransferLedgerMustMatch      = 24
	errPendingTransferNotFound      = 25
	errPendingTransferAlreadyPosted = 33
	errPendingTransferAlreadyVoided = 34
	errExists                       = 46
	errExceedsCredits               = 54
	errExceedsDebits                = 55
)

// Account flags
const (
	accountFlagLinked                       uint16 = 0x0001
	accountFlagDebitsMustNotExceedCredits   uint16 = 0x0002
	accountFlagCreditsMustNotExceedDebits   uint16 = 0x0004
	accountFlagHistory                      uint16 = 0x0008
)

// Transfer flags
const (
	transferFlagLinked      uint16 = 0x0001
	transferFlagPending     uint16 = 0x0002
	transferFlagPostPending uint16 = 0x0004
	transferFlagVoidPending uint16 = 0x0008
)

// AccountFilter flags (for get_account_transfers / get_account_balances)
const (
	filterDebits   uint32 = 0x01
	filterCredits  uint32 = 0x02
	filterReversed uint32 = 0x04
)

// ============================================================================
// Test runner
// ============================================================================

type runner struct {
	client  tb.Client
	passed  int
	failed  int
	start   time.Time
	counter uint64 // monotonic ID source
}

func newRunner(address string) (*runner, error) {
	clusterID := types.ToUint128(0)
	client, err := tb.NewClient(clusterID, []string{address})
	if err != nil {
		return nil, fmt.Errorf("connect to TigerBeetle@%s: %w", address, err)
	}
	return &runner{client: client, start: time.Now()}, nil
}

func (r *runner) close() { r.client.Close() }

// nextID returns a fresh monotonically-increasing uint128 ID.
func (r *runner) nextID() types.Uint128 {
	r.counter++
	return types.ToUint128(r.counter)
}

// createAccounts submits a batch; returns the error results slice.
func (r *runner) createAccounts(accounts []types.Account) []types.AccountEventResult {
	res, err := r.client.CreateAccounts(accounts)
	if err != nil {
		panic(fmt.Sprintf("CreateAccounts RPC error: %v", err))
	}
	return res
}

// createTransfers submits a batch; returns the error results slice.
func (r *runner) createTransfers(transfers []types.Transfer) []types.TransferEventResult {
	res, err := r.client.CreateTransfers(transfers)
	if err != nil {
		panic(fmt.Sprintf("CreateTransfers RPC error: %v", err))
	}
	return res
}

// lookupAccount returns the account or nil.
func (r *runner) lookupAccount(id types.Uint128) *types.Account {
	accs, err := r.client.LookupAccounts([]types.Uint128{id})
	if err != nil || len(accs) == 0 {
		return nil
	}
	return &accs[0]
}

// lookupTransfer returns the transfer or nil.
func (r *runner) lookupTransfer(id types.Uint128) *types.Transfer {
	txs, err := r.client.LookupTransfers([]types.Uint128{id})
	if err != nil || len(txs) == 0 {
		return nil
	}
	return &txs[0]
}

// getAccountTransfers fetches transfers for an account.
// TimestampMax=0 means "no upper bound" in TigerBeetle.
func (r *runner) getAccountTransfers(accountID types.Uint128, limit uint32, flags uint32) []types.Transfer {
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: 0,
		TimestampMax: 0, // 0 = no upper bound
		Limit:        limit,
		Flags: (types.AccountFilterFlags{
			Debits:   flags&filterDebits != 0,
			Credits:  flags&filterCredits != 0,
			Reversed: flags&filterReversed != 0,
		}).ToUint32(),
	}
	txs, err := r.client.GetAccountTransfers(filter)
	if err != nil {
		return nil
	}
	return txs
}

// getAccountBalances fetches balance history for an account.
// TimestampMax=0 means "no upper bound" in TigerBeetle.
func (r *runner) getAccountBalances(accountID types.Uint128, limit uint32) []types.AccountBalance {
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: 0,
		TimestampMax: 0, // 0 = no upper bound
		Limit:        limit,
		Flags: (types.AccountFilterFlags{
			Debits:  true,
			Credits: true,
		}).ToUint32(),
	}
	bals, err := r.client.GetAccountBalances(filter)
	if err != nil {
		return nil
	}
	return bals
}

// resultCode extracts the error code from a single-element result slice.
// Returns errOK (0) if results is empty (all succeeded).
func resultCode(results []types.AccountEventResult, idx int) uint32 {
	for _, r := range results {
		if int(r.Index) == idx {
			return uint32(r.Result)
		}
	}
	return errOK
}

func transferResultCode(results []types.TransferEventResult, idx int) uint32 {
	for _, r := range results {
		if int(r.Index) == idx {
			return uint32(r.Result)
		}
	}
	return errOK
}

// ============================================================================
// Test helpers
// ============================================================================

func (r *runner) runTest(name string, fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("  FAIL: %s — panic: %v\n", name, rec)
			r.failed++
		}
	}()
	fn()
}

func (r *runner) pass(name string) {
	fmt.Printf("  PASS: %s\n", name)
	r.passed++
}

func (r *runner) fail(name, reason string) {
	fmt.Printf("  FAIL: %s — %s\n", name, reason)
	r.failed++
}

func (r *runner) assert(name string, ok bool, msg string) bool {
	if !ok {
		r.fail(name, msg)
		return false
	}
	return true
}

// makeAccount builds a simple account struct.
func makeAccount(id types.Uint128, ledger uint32, code uint16, flags uint16) types.Account {
	return types.Account{
		ID:     id,
		Ledger: ledger,
		Code:   code,
		Flags:  types.AccountFlags{}.ToUint16() | flags,
	}
}

// u128 is a convenience alias for types.ToUint128.
func u128(n uint64) types.Uint128 { return types.ToUint128(n) }

// ============================================================================
// Test cases
// ============================================================================

func (r *runner) testCreateAccountBasic() {
	name := "Create account (basic)"
	id := r.nextID()
	results := r.createAccounts([]types.Account{makeAccount(id, 700, 10, 0)})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("create failed: %v", results)) {
		return
	}
	acc := r.lookupAccount(id)
	if !r.assert(name, acc != nil, "account not found after creation") {
		return
	}
	if !r.assert(name, acc.Ledger == 700, fmt.Sprintf("ledger mismatch: %d", acc.Ledger)) {
		return
	}
	if !r.assert(name, acc.DebitsPosted == u128(0), "debits_posted should be 0") {
		return
	}
	if !r.assert(name, acc.CreditsPosted == u128(0), "credits_posted should be 0") {
		return
	}
	r.pass(name)
}

func (r *runner) testDuplicateAccountRejected() {
	name := "Duplicate account rejected"
	id := r.nextID()
	r.createAccounts([]types.Account{makeAccount(id, 700, 10, 0)})
	results := r.createAccounts([]types.Account{makeAccount(id, 700, 10, 0)})
	code := resultCode(results, 0)
	if !r.assert(name, code == errIDAlreadyExists,
		fmt.Sprintf("expected ID_ALREADY_EXISTS(%d), got %d", errIDAlreadyExists, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testLinkedAccountsSuccess() {
	name := "Linked accounts — success path"
	id1, id2, id3 := r.nextID(), r.nextID(), r.nextID()
	batch := []types.Account{
		{ID: id1, Ledger: 700, Code: 10, Flags: accountFlagLinked},
		{ID: id2, Ledger: 700, Code: 10, Flags: accountFlagLinked},
		{ID: id3, Ledger: 700, Code: 10, Flags: 0},
	}
	results := r.createAccounts(batch)
	if !r.assert(name, len(results) == 0, fmt.Sprintf("linked create failed: %v", results)) {
		return
	}
	for _, id := range []types.Uint128{id1, id2, id3} {
		if !r.assert(name, r.lookupAccount(id) != nil, fmt.Sprintf("account %v not found", id)) {
			return
		}
	}
	r.pass(name)
}

func (r *runner) testLinkedAccountsRollback() {
	name := "Linked accounts — rollback on failure"
	// Pre-create an account that will cause the chain to fail
	existingID := r.nextID()
	r.createAccounts([]types.Account{makeAccount(existingID, 700, 10, 0)})

	newID := r.nextID()
	batch := []types.Account{
		{ID: newID, Ledger: 700, Code: 10, Flags: accountFlagLinked},
		{ID: existingID, Ledger: 700, Code: 10, Flags: 0}, // duplicate → chain fails
	}
	r.createAccounts(batch)

	// newID should NOT have been created (rolled back)
	acc := r.lookupAccount(newID)
	if !r.assert(name, acc == nil, "account should have been rolled back but exists") {
		return
	}
	r.pass(name)
}

func (r *runner) testSimpleTransfer() {
	name := "Simple direct transfer"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{
		makeAccount(dID, 700, 10, 0),
		makeAccount(cID, 700, 10, 0),
	})
	results := r.createTransfers([]types.Transfer{{
		ID:              r.nextID(),
		DebitAccountID:  dID,
		CreditAccountID: cID,
		Amount:          u128(1000),
		Ledger:          700,
		Code:            10,
	}})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("transfer failed: %v", results)) {
		return
	}
	da := r.lookupAccount(dID)
	ca := r.lookupAccount(cID)
	if !r.assert(name, da.DebitsPosted == u128(1000), "debit account debits_posted mismatch") {
		return
	}
	if !r.assert(name, ca.CreditsPosted == u128(1000), "credit account credits_posted mismatch") {
		return
	}
	r.pass(name)
}

func (r *runner) testTransferNonexistentDebitAccount() {
	name := "Transfer: nonexistent debit account"
	cID := r.nextID()
	r.createAccounts([]types.Account{makeAccount(cID, 700, 10, 0)})
	results := r.createTransfers([]types.Transfer{{
		ID:              r.nextID(),
		DebitAccountID:  r.nextID(), // does not exist
		CreditAccountID: cID,
		Amount:          u128(100),
		Ledger:          700,
		Code:            10,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errDebitAccountNotFound,
		fmt.Sprintf("expected DEBIT_ACCOUNT_NOT_FOUND(%d), got %d", errDebitAccountNotFound, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testTransferNonexistentCreditAccount() {
	name := "Transfer: nonexistent credit account"
	dID := r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0)})
	results := r.createTransfers([]types.Transfer{{
		ID:              r.nextID(),
		DebitAccountID:  dID,
		CreditAccountID: r.nextID(), // does not exist
		Amount:          u128(100),
		Ledger:          700,
		Code:            10,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errCreditAccountNotFound,
		fmt.Sprintf("expected CREDIT_ACCOUNT_NOT_FOUND(%d), got %d", errCreditAccountNotFound, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testTwoPhasePending() {
	name := "Two-phase: PENDING"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	results := r.createTransfers([]types.Transfer{{
		ID:              r.nextID(),
		DebitAccountID:  dID,
		CreditAccountID: cID,
		Amount:          u128(500),
		Ledger:          700,
		Code:            10,
		Flags:           transferFlagPending,
	}})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("pending transfer failed: %v", results)) {
		return
	}
	da := r.lookupAccount(dID)
	ca := r.lookupAccount(cID)
	if !r.assert(name, da.DebitsPending == u128(500), "debit pending mismatch") {
		return
	}
	if !r.assert(name, da.DebitsPosted == u128(0), "debit posted should be 0") {
		return
	}
	if !r.assert(name, ca.CreditsPending == u128(500), "credit pending mismatch") {
		return
	}
	r.pass(name)
}

func (r *runner) testTwoPhasePost() {
	name := "Two-phase: PENDING → POST"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	pendingID := r.nextID()
	r.createTransfers([]types.Transfer{{
		ID: pendingID, DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(600), Ledger: 700, Code: 10, Flags: transferFlagPending,
	}})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(600), Ledger: 700, Code: 10,
		Flags: transferFlagPostPending, PendingID: pendingID,
	}})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("post failed: %v", results)) {
		return
	}
	da := r.lookupAccount(dID)
	ca := r.lookupAccount(cID)
	if !r.assert(name, da.DebitsPending == u128(0), "pending should be cleared") {
		return
	}
	if !r.assert(name, da.DebitsPosted == u128(600), "debits_posted mismatch") {
		return
	}
	if !r.assert(name, ca.CreditsPosted == u128(600), "credits_posted mismatch") {
		return
	}
	r.pass(name)
}

func (r *runner) testTwoPhaseVoid() {
	name := "Two-phase: PENDING → VOID"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	pendingID := r.nextID()
	r.createTransfers([]types.Transfer{{
		ID: pendingID, DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(700), Ledger: 700, Code: 10, Flags: transferFlagPending,
	}})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(700), Ledger: 700, Code: 10,
		Flags: transferFlagVoidPending, PendingID: pendingID,
	}})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("void failed: %v", results)) {
		return
	}
	da := r.lookupAccount(dID)
	ca := r.lookupAccount(cID)
	if !r.assert(name, da.DebitsPending == u128(0), "pending should be cleared") {
		return
	}
	if !r.assert(name, da.DebitsPosted == u128(0), "debits_posted should be 0") {
		return
	}
	if !r.assert(name, ca.CreditsPosted == u128(0), "credits_posted should be 0") {
		return
	}
	r.pass(name)
}

func (r *runner) testDoublePostRejected() {
	name := "Double-post rejected"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	pendingID := r.nextID()
	r.createTransfers([]types.Transfer{{
		ID: pendingID, DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10, Flags: transferFlagPending,
	}})
	r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10,
		Flags: transferFlagPostPending, PendingID: pendingID,
	}})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10,
		Flags: transferFlagPostPending, PendingID: pendingID,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errPendingTransferAlreadyPosted,
		fmt.Sprintf("expected ALREADY_POSTED(%d), got %d", errPendingTransferAlreadyPosted, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testVoidAfterPostRejected() {
	name := "Void-after-post rejected"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	pendingID := r.nextID()
	r.createTransfers([]types.Transfer{{
		ID: pendingID, DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10, Flags: transferFlagPending,
	}})
	r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10,
		Flags: transferFlagPostPending, PendingID: pendingID,
	}})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(100), Ledger: 700, Code: 10,
		Flags: transferFlagVoidPending, PendingID: pendingID,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errPendingTransferAlreadyPosted,
		fmt.Sprintf("expected ALREADY_POSTED(%d), got %d", errPendingTransferAlreadyPosted, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testBalanceConstraintDebitsMustNotExceedCredits() {
	name := "Balance constraint: DEBITS_MUST_NOT_EXCEED_CREDITS"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{
		{ID: dID, Ledger: 700, Code: 10, Flags: accountFlagDebitsMustNotExceedCredits},
		makeAccount(cID, 700, 10, 0),
	})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(500), Ledger: 700, Code: 10,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errExceedsCredits,
		fmt.Sprintf("expected EXCEEDS_CREDITS(%d), got %d", errExceedsCredits, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testBalanceConstraintCreditsMustNotExceedDebits() {
	name := "Balance constraint: CREDITS_MUST_NOT_EXCEED_DEBITS"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{
		makeAccount(dID, 700, 10, 0),
		{ID: cID, Ledger: 700, Code: 10, Flags: accountFlagCreditsMustNotExceedDebits},
	})
	results := r.createTransfers([]types.Transfer{{
		ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(500), Ledger: 700, Code: 10,
	}})
	code := transferResultCode(results, 0)
	if !r.assert(name, code == errExceedsDebits,
		fmt.Sprintf("expected EXCEEDS_DEBITS(%d), got %d", errExceedsDebits, code)) {
		return
	}
	r.pass(name)
}

func (r *runner) testLinkedTransfersSuccess() {
	name := "Linked transfers — success path"
	a1, a2, a3 := r.nextID(), r.nextID(), r.nextID()
	r.createAccounts([]types.Account{
		makeAccount(a1, 700, 10, 0),
		makeAccount(a2, 700, 10, 0),
		makeAccount(a3, 700, 10, 0),
	})
	results := r.createTransfers([]types.Transfer{
		{ID: r.nextID(), DebitAccountID: a1, CreditAccountID: a2,
			Amount: u128(100), Ledger: 700, Code: 10, Flags: transferFlagLinked},
		{ID: r.nextID(), DebitAccountID: a2, CreditAccountID: a3,
			Amount: u128(50), Ledger: 700, Code: 10},
	})
	if !r.assert(name, len(results) == 0, fmt.Sprintf("linked transfers failed: %v", results)) {
		return
	}
	acc1 := r.lookupAccount(a1)
	acc2 := r.lookupAccount(a2)
	acc3 := r.lookupAccount(a3)
	if !r.assert(name, acc1.DebitsPosted == u128(100), "a1 debits_posted mismatch") {
		return
	}
	if !r.assert(name, acc2.CreditsPosted == u128(100), "a2 credits_posted mismatch") {
		return
	}
	if !r.assert(name, acc2.DebitsPosted == u128(50), "a2 debits_posted mismatch") {
		return
	}
	if !r.assert(name, acc3.CreditsPosted == u128(50), "a3 credits_posted mismatch") {
		return
	}
	r.pass(name)
}

func (r *runner) testLinkedTransfersRollback() {
	name := "Linked transfers — rollback on failure"
	a1, a2 := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(a1, 700, 10, 0), makeAccount(a2, 700, 10, 0)})
	r.createTransfers([]types.Transfer{
		{ID: r.nextID(), DebitAccountID: a1, CreditAccountID: a2,
			Amount: u128(200), Ledger: 700, Code: 10, Flags: transferFlagLinked},
		{ID: r.nextID(), DebitAccountID: a1, CreditAccountID: r.nextID(), // nonexistent
			Amount: u128(50), Ledger: 700, Code: 10},
	})
	acc1 := r.lookupAccount(a1)
	acc2 := r.lookupAccount(a2)
	if !r.assert(name, acc1.DebitsPosted == u128(0), "debits should be 0 after rollback") {
		return
	}
	if !r.assert(name, acc2.CreditsPosted == u128(0), "credits should be 0 after rollback") {
		return
	}
	r.pass(name)
}

func (r *runner) testLookupTransfer() {
	name := "Lookup transfer by ID"
	dID, cID := r.nextID(), r.nextID()
	txID := r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	r.createTransfers([]types.Transfer{{
		ID: txID, DebitAccountID: dID, CreditAccountID: cID,
		Amount: u128(250), Ledger: 700, Code: 10,
	}})
	tx := r.lookupTransfer(txID)
	if !r.assert(name, tx != nil, "transfer not found") {
		return
	}
	if !r.assert(name, tx.DebitAccountID == dID, "debit account mismatch") {
		return
	}
	if !r.assert(name, tx.CreditAccountID == cID, "credit account mismatch") {
		return
	}
	if !r.assert(name, tx.Amount == u128(250), "amount mismatch") {
		return
	}
	r.pass(name)
}

func (r *runner) testGetAccountTransfers() {
	name := "GET_ACCOUNT_TRANSFERS query"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	for i := 0; i < 5; i++ {
		r.createTransfers([]types.Transfer{{
			ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
			Amount: u128(uint64(100 * (i + 1))), Ledger: 700, Code: 10,
		}})
	}
	// All
	txs := r.getAccountTransfers(dID, 10, filterDebits|filterCredits)
	if !r.assert(name, len(txs) == 5, fmt.Sprintf("expected 5 transfers, got %d", len(txs))) {
		return
	}
	// Limit
	txsLim := r.getAccountTransfers(dID, 3, filterDebits)
	if !r.assert(name, len(txsLim) == 3, fmt.Sprintf("expected 3 (limit), got %d", len(txsLim))) {
		return
	}
	// Credits from other side
	txsC := r.getAccountTransfers(cID, 10, filterCredits)
	if !r.assert(name, len(txsC) == 5, fmt.Sprintf("expected 5 credits, got %d", len(txsC))) {
		return
	}
	r.pass(name)
}

func (r *runner) testGetAccountBalances() {
	name := "GET_ACCOUNT_BALANCES query"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{
		{ID: dID, Ledger: 700, Code: 10, Flags: accountFlagHistory},
		makeAccount(cID, 700, 10, 0),
	})
	for i := 0; i < 3; i++ {
		r.createTransfers([]types.Transfer{{
			ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
			Amount: u128(100), Ledger: 700, Code: 10,
		}})
	}
	bals := r.getAccountBalances(dID, 10)
	if !r.assert(name, len(bals) == 3, fmt.Sprintf("expected 3 snapshots, got %d", len(bals))) {
		return
	}
	// Verify cumulative
	if !r.assert(name, bals[0].DebitsPosted == u128(100), "snapshot 1 mismatch") {
		return
	}
	if !r.assert(name, bals[1].DebitsPosted == u128(200), "snapshot 2 mismatch") {
		return
	}
	if !r.assert(name, bals[2].DebitsPosted == u128(300), "snapshot 3 mismatch") {
		return
	}
	// Account without HISTORY → empty
	balsNoHist := r.getAccountBalances(cID, 10)
	if !r.assert(name, len(balsNoHist) == 0, "non-history account should return no snapshots") {
		return
	}
	r.pass(name)
}

func (r *runner) testMultipleTransfersCumulative() {
	name := "Multiple transfers — cumulative balances"
	dID, cID := r.nextID(), r.nextID()
	r.createAccounts([]types.Account{makeAccount(dID, 700, 10, 0), makeAccount(cID, 700, 10, 0)})
	for i := 0; i < 10; i++ {
		r.createTransfers([]types.Transfer{{
			ID: r.nextID(), DebitAccountID: dID, CreditAccountID: cID,
			Amount: u128(100), Ledger: 700, Code: 10,
		}})
	}
	da := r.lookupAccount(dID)
	ca := r.lookupAccount(cID)
	if !r.assert(name, da.DebitsPosted == u128(1000), fmt.Sprintf("debits_posted: %v", da.DebitsPosted)) {
		return
	}
	if !r.assert(name, ca.CreditsPosted == u128(1000), fmt.Sprintf("credits_posted: %v", ca.CreditsPosted)) {
		return
	}
	r.pass(name)
}

// ============================================================================
// Main
// ============================================================================

func (r *runner) runAll() bool {
	fmt.Printf("\n%s\n", repeat('=', 60))
	fmt.Println("TigerBeetle Sanity Tests — TigerBeetle (native)")
	fmt.Printf("%s\n\n", repeat('=', 60))

	tests := []struct {
		name string
		fn   func()
	}{
		{"Create account (basic)", r.testCreateAccountBasic},
		{"Duplicate account rejected", r.testDuplicateAccountRejected},
		{"Linked accounts — success path", r.testLinkedAccountsSuccess},
		{"Linked accounts — rollback on failure", r.testLinkedAccountsRollback},
		{"Simple direct transfer", r.testSimpleTransfer},
		{"Transfer: nonexistent debit account", r.testTransferNonexistentDebitAccount},
		{"Transfer: nonexistent credit account", r.testTransferNonexistentCreditAccount},
		{"Two-phase: PENDING", r.testTwoPhasePending},
		{"Two-phase: PENDING → POST", r.testTwoPhasePost},
		{"Two-phase: PENDING → VOID", r.testTwoPhaseVoid},
		{"Double-post rejected", r.testDoublePostRejected},
		{"Void-after-post rejected", r.testVoidAfterPostRejected},
		{"Balance constraint: DEBITS_MUST_NOT_EXCEED_CREDITS", r.testBalanceConstraintDebitsMustNotExceedCredits},
		{"Balance constraint: CREDITS_MUST_NOT_EXCEED_DEBITS", r.testBalanceConstraintCreditsMustNotExceedDebits},
		{"Linked transfers — success path", r.testLinkedTransfersSuccess},
		{"Linked transfers — rollback on failure", r.testLinkedTransfersRollback},
		{"Lookup transfer by ID", r.testLookupTransfer},
		{"GET_ACCOUNT_TRANSFERS query", r.testGetAccountTransfers},
		{"GET_ACCOUNT_BALANCES query", r.testGetAccountBalances},
		{"Multiple transfers — cumulative balances", r.testMultipleTransfersCumulative},
	}

	for _, tc := range tests {
		r.runTest(tc.name, tc.fn)
	}

	elapsed := time.Since(r.start)
	fmt.Printf("\n%s\n", repeat('=', 60))
	fmt.Printf("Results: %d passed, %d failed  (%.2fs)\n",
		r.passed, r.failed, elapsed.Seconds())
	fmt.Printf("%s\n\n", repeat('=', 60))
	return r.failed == 0
}

func repeat(ch rune, n int) string {
	buf := make([]rune, n)
	for i := range buf {
		buf[i] = ch
	}
	return string(buf)
}

func main() {
	address := flag.String("address", "3000", "TigerBeetle replica address (e.g. localhost:3000 or just 3000)")
	flag.Parse()

	r, err := newRunner(*address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer r.close()

	ok := r.runAll()
	if !ok {
		os.Exit(1)
	}
}
