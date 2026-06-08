UPDATE public.action
SET status = 'open'
WHERE status IS NOT NULL
  AND lower(status) = 'open';

UPDATE public.action
SET status = 'blocked'
WHERE status IS NOT NULL
  AND lower(status) = 'blocked';

UPDATE public.action
SET status = 'closed'
WHERE status IS NOT NULL
  AND lower(status) IN ('closed', 'completed', 'complete');
