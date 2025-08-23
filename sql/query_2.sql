SELECT login,
       name,
       email,
       bio,
       company,
       location,
       created_at,
       is_hireable,
       repositories_count,
       followers_count,
       following_count,
       twitter_username,
       website_url,
       status
FROM public.users
LIMIT 30000;