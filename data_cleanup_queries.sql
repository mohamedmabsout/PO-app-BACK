-- =====================================================================
-- HISTORICAL DATA CLEANUP QUERIES FOR ADVANCE DOUBLE-APPLICATION BUG
-- =====================================================================
-- Run these queries to fix historical data affected by:
-- 1. SBCAdvance.created_at using server_default instead of payment_confirmed_at
-- 2. Advance double-application between APPROVED_L2 and PAID states
-- =====================================================================

-- =====================================================================
-- PART 1: Fix SBCAdvance.created_at timestamps
-- =====================================================================
-- This fixes the date bug where old advances showed wrong dates in ledger

-- 1.1 PREVIEW: Find SBCAdvance records with mismatched dates
SELECT
    sa.id as sbc_advance_id,
    sa.expense_id,
    e.amount,
    e.payment_confirmed_at as correct_date,
    sa.created_at as incorrect_date,
    DATEDIFF(DAY, e.payment_confirmed_at, sa.created_at) as days_off,
    s.short_name as sbc_name
FROM sbc_advances sa
JOIN expenses e ON sa.expense_id = e.id
JOIN sbcs s ON sa.sbc_id = s.id
WHERE e.exp_type = 'AVANCE_SBC'
  AND e.status IN ('PAID', 'ACKNOWLEDGED')
  AND sa.created_at != e.payment_confirmed_at
ORDER BY days_off DESC;

-- 1.2 FIX: Update SBCAdvance.created_at to match payment_confirmed_at
-- BACKUP YOUR DATABASE BEFORE RUNNING THIS!
UPDATE sbc_advances sa
SET sa.created_at = (
    SELECT e.payment_confirmed_at
    FROM expenses e
    WHERE e.id = sa.expense_id
)
WHERE sa.expense_id IN (
    SELECT e.id
    FROM expenses e
    WHERE e.exp_type = 'AVANCE_SBC'
      AND e.status IN ('PAID', 'ACKNOWLEDGED')
      AND sbc_advances.created_at != e.payment_confirmed_at
);

-- =====================================================================
-- PART 2: Detect historical double-application issues
-- =====================================================================
-- Find expenses where the same advance may have been applied twice

-- 2.1 PREVIEW: Find SBCs with potentially overlapping deductions
SELECT
    s.id as sbc_id,
    s.short_name,
    COUNT(DISTINCT sa.id) as total_advances,
    SUM(sa.amount) as total_advance_pool,
    COUNT(DISTINCT exp_acpt.id) as num_acceptance_expenses,
    SUM(CASE WHEN exp_acpt.exp_type = 'ACCEPTANCE_PP' THEN 1 ELSE 0 END) as num_with_deductions
FROM sbcs s
LEFT JOIN sbc_advances sa ON s.id = sa.sbc_id
LEFT JOIN expenses exp_acpt ON s.id = exp_acpt.sbc_id
    AND exp_acpt.exp_type = 'ACCEPTANCE_PP'
    AND exp_acpt.status IN ('PAID', 'ACKNOWLEDGED')
GROUP BY s.id, s.short_name
HAVING COUNT(DISTINCT exp_acpt.id) > 1
ORDER BY s.short_name;

-- 2.2 DETAIL: For each SBC with multiple ACCEPTANCE_PP expenses, show deductions
SELECT
    s.short_name as sbc_name,
    exp.id as expense_id,
    exp.created_at,
    exp.payment_confirmed_at,
    exp.status,
    SUM(sa.total_amount_ht) as acts_gross_total,
    exp.amount as net_paid,
    SUM(sa.total_amount_ht) - exp.amount as advance_deduction_claimed,
    exp.remark
FROM expenses exp
JOIN sbcs s ON exp.sbc_id = s.id
LEFT JOIN service_acceptances sa ON exp.id = sa.expense_id
WHERE exp.exp_type = 'ACCEPTANCE_PP'
  AND exp.status IN ('PAID', 'ACKNOWLEDGED')
GROUP BY exp.id, s.short_name, exp.created_at, exp.payment_confirmed_at, exp.status, exp.amount, exp.remark
ORDER BY s.short_name, exp.created_at;

-- 2.3 DETAIL: Show which advances were consumed in what order
SELECT
    sa.id as advance_id,
    sa.expense_id as source_expense_id,
    e_source.created_at as advance_created_at,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    s.short_name as sbc_name
FROM sbc_advances sa
JOIN expenses e_source ON sa.expense_id = e_source.id
JOIN sbcs s ON sa.sbc_id = s.id
ORDER BY s.short_name, sa.created_at;

-- =====================================================================
-- PART 3: Verification Queries (Run AFTER fixes)
-- =====================================================================

-- 3.1 Verify all SBCAdvance.created_at now matches expense.payment_confirmed_at
SELECT
    sa.id,
    sa.created_at,
    e.payment_confirmed_at,
    CASE WHEN sa.created_at = e.payment_confirmed_at THEN 'OK' ELSE 'MISMATCH' END as status
FROM sbc_advances sa
JOIN expenses e ON sa.expense_id = e.id
WHERE e.exp_type = 'AVANCE_SBC'
  AND e.status IN ('PAID', 'ACKNOWLEDGED')
ORDER BY status DESC, sa.created_at;

-- 3.2 Verify no unconsumed advances exist (unless recent)
SELECT
    s.short_name,
    sa.id,
    sa.amount,
    sa.remaining_amount,
    sa.is_consumed,
    sa.created_at
FROM sbc_advances sa
JOIN sbcs s ON sa.sbc_id = s.id
WHERE sa.is_consumed = False
  AND sa.created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY s.short_name, sa.created_at;

-- 3.3 Summary: Total pool balance by SBC (should match ledger)
SELECT
    s.short_name,
    COUNT(sa.id) as num_advances,
    SUM(sa.amount) as total_advanced,
    SUM(CASE WHEN sa.is_consumed = True THEN sa.amount ELSE 0 END) as total_consumed,
    SUM(sa.remaining_amount) as total_remaining_balance
FROM sbcs s
LEFT JOIN sbc_advances sa ON s.id = sa.sbc_id
GROUP BY s.id, s.short_name
ORDER BY total_remaining_balance DESC;

-- =====================================================================
-- PART 4: Optional - Rollback (if something goes wrong)
-- =====================================================================
-- If you need to rollback the created_at fix, you can run this:
-- (This is just informational - requires manual transaction in your app)

-- BACKUP of original bad timestamps (before cleanup)
-- SELECT sa.id, sa.created_at as old_value FROM sbc_advances sa
-- WHERE sa.created_at != (SELECT e.payment_confirmed_at FROM expenses e WHERE e.id = sa.expense_id);
