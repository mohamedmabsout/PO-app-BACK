-- =====================================================================
-- VERIFICATION & END-TO-END TEST QUERIES
-- =====================================================================
-- Run these to verify Fix #2 (advance double-application) is working correctly
-- =====================================================================

-- =====================================================================
-- TEST SCENARIO 1: Verify SBCAdvance Consumption at Creation Time
-- =====================================================================
-- When an ACCEPTANCE_PP expense is CREATED, advances should be marked as consumed immediately

-- 1.1 Find a recent ACCEPTANCE_PP expense
SELECT TOP 1
    e.id as expense_id,
    e.created_at as expense_created,
    e.status,
    s.short_name as sbc_name,
    SUM(sa_ref.total_amount_ht) as acts_gross,
    e.amount as net_amount,
    SUM(sa_ref.total_amount_ht) - e.amount as claimed_deduction,
    e.remark
FROM expenses e
JOIN sbcs s ON e.sbc_id = s.id
LEFT JOIN service_acceptances sa_ref ON e.id = sa_ref.expense_id
WHERE e.exp_type = 'ACCEPTANCE_PP'
  AND e.status IN ('DRAFT', 'SUBMITTED', 'PENDING_L1', 'PENDING_L2', 'APPROVED_L2', 'PAID', 'ACKNOWLEDGED')
GROUP BY e.id, e.created_at, e.status, s.short_name, e.amount, e.remark
ORDER BY e.created_at DESC;

-- 1.2 For that expense, check which advances are marked as consumed
-- (Run with the expense_id from 1.1)
DECLARE @expense_id INT = 250;  -- REPLACE WITH ACTUAL EXPENSE ID

SELECT
    sa.id as advance_id,
    sa.expense_id as source_expense_id,
    e_source.created_at as advance_paid_date,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    CASE
        WHEN sa.is_consumed = 1 AND sa.remaining_amount = 0 THEN 'Fully Consumed'
        WHEN sa.is_consumed = 1 AND sa.remaining_amount > 0 THEN 'ERROR: is_consumed but has remaining'
        WHEN sa.is_consumed = 0 THEN 'Not Consumed'
    END as consumption_status
FROM sbc_advances sa
JOIN expenses e_source ON sa.expense_id = e_source.id
WHERE sa.sbc_id = (SELECT sbc_id FROM expenses WHERE id = @expense_id)
ORDER BY sa.created_at;

-- =====================================================================
-- TEST SCENARIO 2: Verify Advance Restoration on Rejection
-- =====================================================================
-- When an ACCEPTANCE_PP expense is REJECTED, consumed advances should be restored

-- 2.1 Get a PAID ACCEPTANCE_PP expense that has advance deductions
SELECT TOP 1
    e.id as expense_id,
    e.status,
    s.short_name as sbc_name,
    e.created_at,
    e.payment_confirmed_at,
    SUM(sa_ref.total_amount_ht) as acts_gross,
    e.amount as net_paid,
    SUM(sa_ref.total_amount_ht) - e.amount as deduction_amount,
    e.remark
FROM expenses e
JOIN sbcs s ON e.sbc_id = s.id
LEFT JOIN service_acceptances sa_ref ON e.id = sa_ref.expense_id
WHERE e.exp_type = 'ACCEPTANCE_PP'
  AND e.status = 'PAID'
  AND (SUM(sa_ref.total_amount_ht) - e.amount) > 0
GROUP BY e.id, e.status, s.short_name, e.created_at, e.payment_confirmed_at, e.amount, e.remark
ORDER BY e.created_at DESC;

-- 2.2 After rejecting that expense, verify advances are restored
-- The consumed advances should be marked is_consumed=False and remaining_amount restored
DECLARE @rejected_expense_id INT = 250;  -- REPLACE WITH ACTUAL EXPENSE ID

SELECT
    sa.id,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    CASE
        WHEN sa.is_consumed = 0 THEN 'OK: Restored'
        WHEN sa.is_consumed = 1 AND sa.remaining_amount = sa.amount THEN 'OK: Fully Restored'
        ELSE 'ERROR: Not properly restored'
    END as restoration_status
FROM sbc_advances sa
WHERE sa.sbc_id = (SELECT sbc_id FROM expenses WHERE id = @rejected_expense_id)
ORDER BY sa.created_at;

-- =====================================================================
-- TEST SCENARIO 3: Verify No Double-Application for ST MOUSTAPHA (sbc_id=24)
-- =====================================================================
-- EXP-174 (7,000) should ONLY appear once in the consumed advances list

-- 3.1 Show all advances for ST MOUSTAPHA with their consumption status
SELECT
    sa.id as advance_id,
    sa.expense_id as source_exp_id,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    e_source.payment_confirmed_at as paid_date,
    e_source.status as source_status
FROM sbc_advances sa
JOIN expenses e_source ON sa.expense_id = e_source.id
WHERE sa.sbc_id = 24
ORDER BY sa.created_at;

-- 3.2 For ST MOUSTAPHA, show all ACCEPTANCE_PP expenses and their deductions
SELECT
    e.id as expense_id,
    e.created_at,
    e.payment_confirmed_at,
    e.status,
    COUNT(sa_ref.id) as num_acts,
    COALESCE(SUM(sa_ref.total_amount_ht), 0) as acts_gross_total,
    e.amount as net_paid,
    COALESCE(SUM(sa_ref.total_amount_ht), 0) - e.amount as deduction_amount,
    CASE WHEN COALESCE(SUM(sa_ref.total_amount_ht), 0) - e.amount > 0
         THEN 'Uses Advances'
         ELSE 'No Advances'
    END as type,
    e.remark
FROM expenses e
LEFT JOIN service_acceptances sa_ref ON e.id = sa_ref.expense_id
WHERE e.sbc_id = 24
  AND e.exp_type = 'ACCEPTANCE_PP'
GROUP BY e.id, e.created_at, e.payment_confirmed_at, e.status, e.amount, e.remark
ORDER BY e.created_at;

-- 3.3 Verify ST MOUSTAPHA's total advance pool and consumption
SELECT
    'TOTAL ADVANCED' as metric,
    SUM(sa.amount) as amount
FROM sbc_advances sa
WHERE sa.sbc_id = 24

UNION ALL

SELECT
    'TOTAL CONSUMED',
    SUM(sa.amount)
FROM sbc_advances sa
WHERE sa.sbc_id = 24 AND sa.is_consumed = 1

UNION ALL

SELECT
    'REMAINING BALANCE',
    SUM(sa.remaining_amount)
FROM sbc_advances sa
WHERE sa.sbc_id = 24;

-- =====================================================================
-- TEST SCENARIO 4: Verify No Advances in Draft State Forever
-- =====================================================================
-- Check for advances that are locked in DRAFT expenses (older than 7 days)

-- 4.1 Find draft ACCEPTANCE_PP expenses with locked advances
SELECT
    e.id as expense_id,
    e.status,
    e.created_at,
    DATEDIFF(DAY, e.created_at, GETDATE()) as days_in_draft,
    s.short_name,
    SUM(sa_ref.total_amount_ht) as acts_gross,
    e.amount,
    SUM(sa_ref.total_amount_ht) - e.amount as locked_amount,
    e.remark
FROM expenses e
JOIN sbcs s ON e.sbc_id = s.id
LEFT JOIN service_acceptances sa_ref ON e.id = sa_ref.expense_id
WHERE e.exp_type = 'ACCEPTANCE_PP'
  AND e.status = 'DRAFT'
  AND DATEDIFF(DAY, e.created_at, GETDATE()) > 7
  AND (SUM(sa_ref.total_amount_ht) - e.amount) > 0
GROUP BY e.id, e.status, e.created_at, s.short_name, e.amount, e.remark
ORDER BY e.created_at;

-- 4.2 Show which advances are locked in those drafts
DECLARE @draft_exp_id INT;  -- Set to a draft expense ID from 4.1

SELECT
    sa.id,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    DATEDIFF(DAY, sa.created_at, GETDATE()) as days_locked
FROM sbc_advances sa
WHERE sa.sbc_id = (SELECT sbc_id FROM expenses WHERE id = @draft_exp_id)
ORDER BY sa.created_at;

-- =====================================================================
-- TEST SCENARIO 5: Global Health Check
-- =====================================================================

-- 5.1 For EACH SBC, verify advance consumption logic is correct
SELECT
    s.short_name as sbc_name,
    COUNT(DISTINCT sa.id) as total_advances,
    SUM(sa.amount) as total_pool,
    SUM(CASE WHEN sa.is_consumed = 1 THEN sa.amount ELSE 0 END) as consumed_total,
    SUM(sa.remaining_amount) as remaining_balance,
    COUNT(DISTINCT CASE WHEN sa.is_consumed = 0 AND DATEDIFF(DAY, sa.created_at, GETDATE()) > 7 THEN sa.id END) as stale_draft_locks,
    COUNT(DISTINCT e_acpt.id) as num_acceptance_expenses,
    COUNT(DISTINCT CASE WHEN e_acpt.status = 'DRAFT' THEN e_acpt.id END) as draft_count,
    COUNT(DISTINCT CASE WHEN e_acpt.status = 'PAID' THEN e_acpt.id END) as paid_count
FROM sbcs s
LEFT JOIN sbc_advances sa ON s.id = sa.sbc_id
LEFT JOIN expenses e_acpt ON s.id = e_acpt.sbc_id AND e_acpt.exp_type = 'ACCEPTANCE_PP'
GROUP BY s.id, s.short_name
ORDER BY total_pool DESC;

-- 5.2 Alert on inconsistencies
SELECT
    s.short_name as sbc_name,
    'Issue: Unconsumed advance too old' as issue_type,
    sa.id as advance_id,
    sa.amount,
    sa.created_at,
    DATEDIFF(DAY, sa.created_at, GETDATE()) as days_old
FROM sbc_advances sa
JOIN sbcs s ON sa.sbc_id = s.id
WHERE sa.is_consumed = 0
  AND DATEDIFF(DAY, sa.created_at, GETDATE()) > 30

UNION ALL

SELECT
    s.short_name,
    'Issue: is_consumed=1 but has remaining_amount',
    sa.id,
    sa.remaining_amount,
    sa.created_at,
    NULL
FROM sbc_advances sa
JOIN sbcs s ON sa.sbc_id = s.id
WHERE sa.is_consumed = 1 AND sa.remaining_amount > 0.01

UNION ALL

SELECT
    s.short_name,
    'Issue: Draft expense with locked advances (>7 days)',
    e.id,
    SUM(sa_ref.total_amount_ht) - e.amount,
    e.created_at,
    DATEDIFF(DAY, e.created_at, GETDATE())
FROM expenses e
JOIN sbcs s ON e.sbc_id = s.id
LEFT JOIN service_acceptances sa_ref ON e.id = sa_ref.expense_id
WHERE e.exp_type = 'ACCEPTANCE_PP'
  AND e.status = 'DRAFT'
  AND DATEDIFF(DAY, e.created_at, GETDATE()) > 7
GROUP BY s.short_name, e.id, e.created_at;
