SELECT login,
       name,
       bio,
       company,
       location,
       created_at,
       followers_count,
       following_count,
       status
FROM public.users
LIMIT 30000;