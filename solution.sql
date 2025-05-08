-- 1.2 Part 1 
-- Insert processed conversion data into fct_client_conversion_events
INSERT INTO fct_client_conversion_events
-- Step 1: Convert user creation time to local time
WITH user_created_at_local AS (
    SELECT
        du.user_id,
        du.branch_id,
         -- Convert user creation timestamp to branch-local timezone
        du.created_at AT TIME ZONE COALESCE(db.timezone_id, 'UTC') AS local_user_created_at
    FROM dim_user du
    JOIN dim_branch db ON du.branch_id = db.branch_id
),
-- Step 2: Get the first membership purchase per user
first_membership AS (
    SELECT DISTINCT ON (mp.user_id)
        mp.user_id,
        mp.branch_id,
        mp.membership_id,
        -- Extract name and source from JSON column
        mp.membership_purchase_details->>'name' AS membership_name,
        mp.membership_purchase_details->>'source' AS membership_source,
        mp.membership_purchased_at,
         -- Convert user creation timestamp to branch-local timezone
        mp.membership_purchased_at AT TIME ZONE COALESCE(db.timezone_id, 'UTC') AS local_membership_purchased_at
    FROM fct_membership_purchases mp
    JOIN dim_branch db ON mp.branch_id = db.branch_id
    WHERE mp.membership_purchased_at IS NOT NULL
    ORDER BY mp.user_id, mp.membership_purchased_at
),
-- Step 3: Get the first credit pack purchase per user
first_credit AS (
    SELECT DISTINCT ON (cp.user_id)
        cp.user_id,
        cp.branch_id,
        cp.credit_pack_id,
        -- Extract name and source from JSON column
        cp.credit_pack_purchase_details->>'name' AS credit_pack_name,
        cp.credit_pack_purchase_details->>'source' AS credit_pack_source,
        cp.credit_pack_purchased_at,
         -- Convert user creation timestamp to branch-local timezone
        cp.credit_pack_purchased_at AT TIME ZONE COALESCE(db.timezone_id, 'UTC') AS local_credit_pack_purchased_at
    FROM fct_credit_pack_purchases cp
    JOIN dim_branch db ON cp.branch_id = db.branch_id
    WHERE cp.credit_pack_purchased_at IS NOT NULL
    ORDER BY cp.user_id, cp.credit_pack_purchased_at
),
-- Step 4: Combine user info with purchase data
combined AS (
    SELECT
        u.user_id,
        u.branch_id,
        u.local_user_created_at,
        -- Determine if user is a client (has made a purchase)
        COALESCE(m.membership_purchased_at, c.credit_pack_purchased_at) IS NOT NULL AS is_client,
        m.membership_id,
        m.local_membership_purchased_at,
        m.membership_name,
        m.membership_source,
        c.credit_pack_id,
        c.local_credit_pack_purchased_at,
        c.credit_pack_name,
        c.credit_pack_source,
         -- Classify event type based on which came first
        CASE
            WHEN m.membership_purchased_at IS NOT NULL AND (c.credit_pack_purchased_at IS NULL OR m.membership_purchased_at < c.credit_pack_purchased_at) THEN 'MEMBERSHIP'
            WHEN c.credit_pack_purchased_at IS NOT NULL THEN 'USER_CREDIT'
            ELSE NULL
        END AS client_conversion_event_type,
        -- Return event ID based on type
        CASE
            WHEN m.membership_purchased_at IS NOT NULL AND (c.credit_pack_purchased_at IS NULL OR m.membership_purchased_at < c.credit_pack_purchased_at) THEN m.membership_id
            WHEN c.credit_pack_purchased_at IS NOT NULL THEN c.credit_pack_id
            ELSE NULL
        END AS client_conversion_event_id,
        -- Return local timestamp of earliest event
        CASE
            WHEN m.membership_purchased_at IS NOT NULL AND (c.credit_pack_purchased_at IS NULL OR m.membership_purchased_at < c.credit_pack_purchased_at) THEN m.local_membership_purchased_at
            WHEN c.credit_pack_purchased_at IS NOT NULL THEN c.local_credit_pack_purchased_at
            ELSE NULL
        END AS client_conversion_event_local_created_at,
        -- Get name of conversion event
        CASE
            WHEN m.membership_purchased_at IS NOT NULL AND (c.credit_pack_purchased_at IS NULL OR m.membership_purchased_at < c.credit_pack_purchased_at) THEN m.membership_name
            WHEN c.credit_pack_purchased_at IS NOT NULL THEN c.credit_pack_name
            ELSE NULL
        END AS client_conversion_event_name,
        -- Get source of conversion event
        CASE
            WHEN m.membership_purchased_at IS NOT NULL AND (c.credit_pack_purchased_at IS NULL OR m.membership_purchased_at < c.credit_pack_purchased_at) THEN m.membership_source
            WHEN c.credit_pack_purchased_at IS NOT NULL THEN c.credit_pack_source
            ELSE NULL
        END AS client_conversion_event_source
    FROM user_created_at_local u
    LEFT JOIN first_membership m ON u.user_id = m.user_id
    LEFT JOIN first_credit c ON u.user_id = c.user_id
)
-- Step 5: Final projection with proper column names
SELECT
    user_id,
    branch_id,
    local_user_created_at,
     -- Mark as CLIENT if any purchase exists, else LEAD
    CASE WHEN is_client THEN 'CLIENT' ELSE 'LEAD' END AS lead_status,
    client_conversion_event_type,
    client_conversion_event_id,
    client_conversion_event_local_created_at,
    client_conversion_event_name,
    client_conversion_event_source,
    membership_id AS first_user_membership_id,
    local_membership_purchased_at AS first_local_membership_purchased_at,
    membership_name AS first_membership_name,
    membership_source AS first_membership_source,
    credit_pack_id AS first_credit_pack_id,
    local_credit_pack_purchased_at AS first_local_credit_pack_purchased_at,
    credit_pack_name AS first_credit_pack_name,
    credit_pack_source AS first_credit_pack_source
FROM combined;

------------------------------------------------------------------------------------------------------------------------------------------------
-- 1.3 Part 2
INSERT INTO fct_lead_conversions
-- Step 1: Load base data from a previously created table
WITH base AS (
    SELECT * FROM fct_client_conversion_events_part_2
),
-- Step 2: Filter only users who purchased a membership
membership_only AS (
    SELECT
        user_id, branch_id, local_user_created_at, lead_status,
        'MEMBERSHIP' AS client_conversion_event_filter,
        first_user_membership_id AS client_conversion_event_id,
        first_local_membership_purchased_at AS client_conversion_event_local_created_at,
        first_membership_name AS client_conversion_event_name,
        first_membership_source AS client_conversion_event_source
    FROM base
    WHERE first_user_membership_id IS NOT NULL
),
-- Step 3: Filter only users who purchased a credit pack
credit_only AS (
    SELECT
        user_id, branch_id, local_user_created_at, lead_status,
        'USER_CREDIT' AS client_conversion_event_filter,
        first_credit_pack_id AS client_conversion_event_id,
        first_local_credit_pack_purchased_at AS client_conversion_event_local_created_at,
        first_credit_pack_name AS client_conversion_event_name,
        first_credit_pack_source AS client_conversion_event_source
    FROM base
    WHERE first_credit_pack_id IS NOT NULL
),
-- Step 4: Combine both membership and credit pack conversions,
-- and pick the earliest one for users who have both
all_combined AS (
    SELECT
        user_id, branch_id, local_user_created_at, lead_status,
        'ALL' AS client_conversion_event_type,
        -- Pick the ID based on whichever conversion happened earlier
        CASE
            WHEN first_local_membership_purchased_at IS NOT NULL AND (
                first_local_credit_pack_purchased_at IS NULL OR
                first_local_membership_purchased_at < first_local_credit_pack_purchased_at)
                THEN first_user_membership_id
            ELSE first_credit_pack_id
        END AS client_conversion_event_id,
        -- Take the earliest conversion time between membership and credit pack
        LEAST(
            COALESCE(first_local_membership_purchased_at, '9999-12-31'),
            COALESCE(first_local_credit_pack_purchased_at, '9999-12-31')
        ) AS client_conversion_event_local_created_at,
        -- Name of the earliest conversion event
        CASE
            WHEN first_local_membership_purchased_at IS NOT NULL AND (
                first_local_credit_pack_purchased_at IS NULL OR
                first_local_membership_purchased_at < first_local_credit_pack_purchased_at)
                THEN first_membership_name
            ELSE first_credit_pack_name
        END AS client_conversion_event_name,
        -- Source of the earliest conversion event
        CASE
            WHEN first_local_membership_purchased_at IS NOT NULL AND (
                first_local_credit_pack_purchased_at IS NULL OR
                first_local_membership_purchased_at < first_local_credit_pack_purchased_at)
                THEN first_membership_source
            ELSE first_credit_pack_source
        END AS client_conversion_event_source
    FROM base
    WHERE first_user_membership_id IS NOT NULL OR first_credit_pack_id IS NOT NULL
)
-- Step 5: Combine all results into a single output
SELECT * FROM membership_only
UNION ALL
SELECT * FROM credit_only
UNION ALL
SELECT * FROM all_combined;




