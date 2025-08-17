SELECT status, COUNT(*) 
FROM public.users 
WHERE status IN ('done', 'pending') 
GROUP BY status;