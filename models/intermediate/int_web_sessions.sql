with events as (
    select 
        user_id,
        id as event_id,
        created_at as event_time,
        event_type,
        uri,
        ip_address,
        browser,
        traffic_source,
        session_id as original_session_id,
        state,
        city
    from {{ ref('stg_events') }}
),

-- Calculate time since previous event for each user
events_with_gaps as (
    select
        *,
        lag(event_time) over (
            partition by user_id 
            order by event_time
        ) as prev_event_time,
        timestamp_diff(
            event_time,
            lag(event_time) over (partition by user_id order by event_time),
            minute
        ) as minutes_since_last_event
    from events
),

-- Mark new sessions (>30 min gap or first event)
session_starts as (
    select
        *,
        case
            when prev_event_time is null then 1
            when minutes_since_last_event > 30 then 1
            else 0
        end as is_new_session
    from events_with_gaps
),

-- Create unique session IDs
sessions_numbered as (
    select
        *,
        sum(is_new_session) over (
            partition by user_id 
            order by event_time 
            rows between unbounded preceding and current row
        ) as user_session_number
    from session_starts
),

-- Generate unique session ID
final_sessions as (
    select
        concat(
            cast(user_id as string), 
            '-', 
            cast(user_session_number as string)
        ) as session_id,
        user_id,
        user_session_number,
        event_id,
        event_time,
        event_type,
        uri,
        traffic_source,
        browser,
        state,
        city,
        is_new_session
    from sessions_numbered
)

select * from final_sessions
order by user_id, event_time