DELETE FROM action_event_log
WHERE event_type ILIKE '%escalation%';

TRUNCATE action_escalation_notification RESTART IDENTITY;
