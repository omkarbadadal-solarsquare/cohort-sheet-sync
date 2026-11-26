/* =========================================================
   COHORT LEVEL QUERY
   One row per Lead ID. Filter = Created Date (IST).
   Window: supplied by Python as an IST inclusive-start / exclusive-end chunk.
   MS / MD / RTM are attributes of the lead created in the window.
   Leads with no meeting have NULL MS / Slot / RTM / Meeting By.
   ========================================================= */

WITH
/* =========================================================
   1. LEADS CREATED IN THE REQUESTED IST CHUNK
   ========================================================= */

LeadRows AS MATERIALIZED (
    SELECT
        l."_id" AS internal_id,
        l.lead_id::text AS lead_id,

        l."createdAt" AT TIME ZONE 'Asia/Kolkata'
            AS created_at_ist,

        COALESCE(
            NULLIF(BTRIM(l.marketing_lead_source::text), ''),
            NULLIF(BTRIM(l.lead_source::text), '')
        ) AS channel,

        l.site_address_pin_code,
        l.site_address_city,

        x.ms_timestamp_ist,
        x.md_timestamp_ist,
        x.meeting_confirmed_timestamp_ist

    FROM public.lead l

    CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN NULLIF(
                    BTRIM(l.meeting_schedule_date::text), ''
                ) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                THEN BTRIM(l.meeting_schedule_date::text)::timestamp
            END AS ms_timestamp_ist,

            CASE
                WHEN NULLIF(
                    BTRIM(l.meeting_done_date::text), ''
                ) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                THEN
                    BTRIM(l.meeting_done_date::text)::timestamp
                        AT TIME ZONE 'UTC'
                        AT TIME ZONE 'Asia/Kolkata'
            END AS md_timestamp_ist,

            CASE
                WHEN NULLIF(
                    BTRIM(l.meeting_confirmed_datetime::text), ''
                ) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                THEN
                    BTRIM(l.meeting_confirmed_datetime::text)::timestamp
                        AT TIME ZONE 'UTC'
                        AT TIME ZONE 'Asia/Kolkata'
            END AS meeting_confirmed_timestamp_ist
    ) x

    WHERE l.lead_id IS NOT NULL
      AND BTRIM(l.lead_id::text) <> ''
      AND (l."createdAt" AT TIME ZONE 'Asia/Kolkata')
            >= TIMESTAMP '__START_DATE_IST__ 00:00:00'
      AND (l."createdAt" AT TIME ZONE 'Asia/Kolkata')
            < TIMESTAMP '__END_DATE_IST__ 00:00:00'
),

/* =========================================================
   2. UNIQUE LEAD ID
   ========================================================= */

CandidateLeads AS MATERIALIZED (
    SELECT DISTINCT ON (lead_id)
        *
    FROM LeadRows

    ORDER BY
        lead_id,
        ms_timestamp_ist DESC NULLS LAST,
        created_at_ist DESC NULLS LAST,
        internal_id DESC
),

/* =========================================================
   3. MEETING HISTORY WITH PREVIOUS SCHEDULED DATE
   Only for leads that have an MS date.
   ========================================================= */

MeetingHistoryWithPrevious AS MATERIALIZED (
    SELECT
        h._id AS history_id,
        h.lead_id::text AS lead_id,

        (
            h.meeting_schedule_date
            AT TIME ZONE 'Asia/Kolkata'
        )::date AS history_scheduled_date,

        (
            h."createdAt"
            AT TIME ZONE 'Asia/Kolkata'
        )::date AS history_created_date,

        h."createdAt" AS history_created_at,
        h."updatedAt" AS history_updated_at,

        LAG(
            (
                h.meeting_schedule_date
                AT TIME ZONE 'Asia/Kolkata'
            )::date
        ) OVER (
            PARTITION BY h.lead_id
            ORDER BY
                h."createdAt",
                h."updatedAt",
                h._id
        ) AS previous_scheduled_date

    FROM public.meeting_metrics_history h

    INNER JOIN (
        SELECT DISTINCT lead_id
        FROM CandidateLeads
        WHERE ms_timestamp_ist IS NOT NULL
    ) cl
        ON h.lead_id::text = cl.lead_id

    WHERE h.lead_id IS NOT NULL
      AND h.meeting_schedule_date IS NOT NULL
      AND h.type = 'meeting_metrics_history'
),

/* =========================================================
   4. MATCH SELECTED MEETING WITH HISTORY
   ========================================================= */

RTMHistoryMatch AS (
    SELECT
        cl.lead_id,
        cl.ms_timestamp_ist,
        cl.meeting_confirmed_timestamp_ist,
        mh.history_created_date,
        mh.previous_scheduled_date,

        ROW_NUMBER() OVER (
            PARTITION BY cl.lead_id
            ORDER BY
                mh.history_created_at DESC NULLS LAST,
                mh.history_updated_at DESC NULLS LAST,
                mh.history_id DESC
        ) AS history_match_rank

    FROM CandidateLeads cl

    LEFT JOIN MeetingHistoryWithPrevious mh
        ON mh.lead_id = cl.lead_id
       AND mh.history_scheduled_date = cl.ms_timestamp_ist::date

    WHERE cl.ms_timestamp_ist IS NOT NULL
),

/* =========================================================
   5. RTM / PRESCHEDULED CLASSIFICATION
   ========================================================= */

RTMTag AS (
    SELECT
        lead_id,

        CASE
            WHEN
                COALESCE(
                    meeting_confirmed_timestamp_ist::date,
                    history_created_date
                ) = ms_timestamp_ist::date

            AND previous_scheduled_date
                    IS DISTINCT FROM ms_timestamp_ist::date

                THEN 'RTM'

            ELSE 'Prescheduled'
        END AS rtm_or_prescheduled

    FROM RTMHistoryMatch

    WHERE history_match_rank = 1
),

/* =========================================================
   6. CLUSTER MAPPING
   ========================================================= */

ClusterMapping AS (
    SELECT
        REGEXP_REPLACE(
            cm.pin_code::text,
            '[^0-9]',
            '',
            'g'
        ) AS pin_code,

        MAX(
            CASE
                WHEN cm.cluster = 'Bengaluru'
                    THEN 'Bangalore'

                WHEN cm.city IN ('Gurgaon', 'Gurugram')
                    THEN 'Gurgaon'

                WHEN cm.city IN (
                    'Noida', 'Gautam Buddha Nagar'
                )
                    THEN 'Noida'

                WHEN cm.city IN (
                    'Agra',
                    'Ghaziabad',
                    'Faridabad',
                    'Vadodara',
                    'Ajmer'
                )
                    THEN cm.city

                ELSE cm.cluster
            END
        ) AS mapped_cluster

    FROM public.city_state_cluster cm

    WHERE cm.pin_code IS NOT NULL

    GROUP BY
        REGEXP_REPLACE(
            cm.pin_code::text,
            '[^0-9]',
            '',
            'g'
        )
),

/* =========================================================
   7. DERIVE CLUSTER
   ========================================================= */

LeadsWithCluster AS (
    SELECT
        cl.*,

        COALESCE(
            CASE
                WHEN UPPER(TRIM(cl.site_address_city)) = 'GUNTUR'
                    THEN 'Guntur'
            END,

            cm.mapped_cluster,

            CASE
                WHEN LOWER(TRIM(cl.site_address_city)) IN (
                    'bengaluru', 'bangalore'
                ) THEN 'Bangalore'

                WHEN LOWER(TRIM(cl.site_address_city)) IN (
                    'gurgaon', 'gurugram'
                ) THEN 'Gurgaon'

                WHEN LOWER(TRIM(cl.site_address_city)) IN (
                    'noida', 'gautam buddha nagar'
                ) THEN 'Noida'

                WHEN LOWER(TRIM(cl.site_address_city)) = 'ghaziabad'
                    THEN 'Ghaziabad'

                WHEN LOWER(TRIM(cl.site_address_city)) = 'faridabad'
                    THEN 'Faridabad'

                WHEN LOWER(TRIM(cl.site_address_city)) = 'pimpri-chinchwad'
                    THEN 'Pune'

                WHEN LOWER(TRIM(cl.site_address_city)) = 'sangareddy'
                    THEN 'Hyderabad'

                WHEN LOWER(TRIM(cl.site_address_city)) = 'tiruvallur'
                    THEN 'Chennai'

                WHEN LOWER(TRIM(cl.site_address_city)) IN (
                    'kanpur', 'kanpur nagar'
                ) THEN 'Kanpur'

                WHEN LOWER(TRIM(cl.site_address_city)) IN (
                    'delhi',
                    'new delhi',
                    'north delhi',
                    'south delhi',
                    'east delhi',
                    'west delhi'
                ) THEN 'Delhi'

                ELSE NULLIF(TRIM(cl.site_address_city), '')
            END,

            'Unknown'
        ) AS cluster

    FROM CandidateLeads cl

    LEFT JOIN ClusterMapping cm
        ON cm.pin_code = REGEXP_REPLACE(
            cl.site_address_pin_code::text,
            '[^0-9]',
            '',
            'g'
        )
),

/* =========================================================
   8. LATEST ANUBIS SCORE PER LEAD
   ========================================================= */

LatestLQI AS MATERIALIZED (
    SELECT DISTINCT ON (s.lead_id)
        s.lead_id,
        s.heuristic_score

    FROM public.anubis_scored_leads s

    INNER JOIN (
        SELECT DISTINCT lead_id
        FROM CandidateLeads
    ) cl
        ON cl.lead_id = s.lead_id

    ORDER BY
        s.lead_id,

        NULLIF(BTRIM(s.scored_at), '')::timestamptz
            DESC NULLS LAST,

        NULLIF(BTRIM(s.lead_updated_at), '')::timestamptz
            DESC NULLS LAST,

        NULLIF(BTRIM(s.lead_created_at), '')::timestamptz
            DESC NULLS LAST
),

/* =========================================================
   9. SIQ WEBHOOK SOURCE
   ========================================================= */

SIQSource AS MATERIALIZED (
    SELECT
        s."leadId" AS internal_id,

        MIN(s."createdAt"::timestamptz) AS ms_date,

        (
            ARRAY_AGG(
                UPPER(TRIM(s.removed_from_siq::text))
                ORDER BY
                    s."createdAt"::timestamptz DESC NULLS LAST
            )
        )[1] AS removed_raw,

        (
            ARRAY_AGG(
                UPPER(TRIM(s.thank_you_page::text))
                ORDER BY
                    s."createdAt"::timestamptz DESC NULLS LAST
            )
        )[1] AS ty_raw

    FROM public.siq_webhook_logs s

    INNER JOIN (
        SELECT DISTINCT internal_id
        FROM CandidateLeads
        WHERE internal_id IS NOT NULL
    ) cl
        ON cl.internal_id = s."leadId"

    GROUP BY s."leadId"
),

/* =========================================================
   10. MEETING BY CLASSIFICATION
   ========================================================= */

MeetingSource AS (
    SELECT
        internal_id,

        CASE
            WHEN ms_date IS NULL
                THEN 'LRM'

            WHEN removed_raw IN ('TRUE', 'T', '1')
             AND ty_raw IN ('TRUE', 'T', '1')
                THEN 'TY Page'

            WHEN removed_raw IN ('FALSE', 'F', '0')
                THEN 'SIQ'

            WHEN removed_raw IN ('TRUE', 'T', '1')
                THEN 'LRM'

            ELSE 'Check / Unknown'
        END AS meeting_by

    FROM SIQSource
)

/* =========================================================
   11. FINAL COHORT RAW DATA
   ========================================================= */

SELECT
    cl.lead_id AS "Lead ID",

    cl.created_at_ist AS "Created Date IST",

    cl.ms_timestamp_ist AS "MS Date IST",

    cl.channel AS "Channel",

    lqi.heuristic_score AS "LQI",

    CASE
        WHEN cl.ms_timestamp_ist IS NULL
            THEN NULL
        WHEN cl.ms_timestamp_ist::time <= TIME '12:00:00'
            THEN 'Slot 1'
        WHEN cl.ms_timestamp_ist::time <= TIME '15:00:00'
            THEN 'Slot 2'
        WHEN cl.ms_timestamp_ist::time <= TIME '18:00:00'
            THEN 'Slot 3'
        ELSE 'Slot 4'
    END AS "Slot",

    cl.cluster AS "Cluster",

    cl.md_timestamp_ist AS "MD Date IST",

    CASE
        WHEN cl.ms_timestamp_ist IS NULL
            THEN NULL
        ELSE COALESCE(ms.meeting_by, 'LRM')
    END AS "Meeting By",

    CASE
        WHEN cl.ms_timestamp_ist IS NULL
            THEN NULL
        WHEN EXTRACT(ISODOW FROM cl.ms_timestamp_ist::date) = 1
            THEN 'Monday'
        WHEN EXTRACT(ISODOW FROM cl.ms_timestamp_ist::date) BETWEEN 2 AND 5
            THEN 'Other Weekday'
        ELSE 'Weekend'
    END AS "MS Day of the Week",

    cl.meeting_confirmed_timestamp_ist
        AS "Meeting Confirmed Date Time IST",

    rtm.rtm_or_prescheduled AS "RTM or Prescheduled",

    /* Temporary cross-chunk tie-breaker; Python removes it before writing Sheet1. */
    cl.internal_id::text AS "_dedupe_internal_id"

FROM LeadsWithCluster cl

LEFT JOIN LatestLQI lqi
    ON lqi.lead_id = cl.lead_id

LEFT JOIN MeetingSource ms
    ON ms.internal_id = cl.internal_id

LEFT JOIN RTMTag rtm
    ON rtm.lead_id = cl.lead_id

ORDER BY
    cl.created_at_ist DESC,
    cl.lead_id;
