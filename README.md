# github-api-scraper-producer-consumer

```bash
docker run \
--name github-app-1 \
--label type=worker \
--network api-github-scraper \
-e GITHUB_API_TOKEN=your-github-api-token \
-e POSTGRES_DSN="postgresql://postgres:postgres@github_postgres:5432/postgres" \
-e REDIS_HOST="redis-container" \
api-github-scraper
```

```bash
docker ps --filter "label=type=worker"
```

```bash
docker run -d --name redisinsight -p 5540:5540 redis/redisinsight:latest
```

```bash
docker ps -a
```

```bash
docker rm container-name
```