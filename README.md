# github-api-scraper-producer-consumer

Important Information ⚠️

If you want to run this app on your MacBook for several hours or days, be sure to keep the following in mind. Connect you MacBook to a power source and then go to:

System settings -> Options

And activate the following option:

"Prevent automatic sleeping on power adapter when the display is off"


```bash
docker build -t api-github-scraper .
```

```bash
docker-compose up -d
```

```bash
docker-compose down -v
```

```bash
docker run \
--name github-app-1 \
--label type=worker \
--network api-github-scraper \
-e GITHUB_API_TOKEN=your-github-api-token \
-e POSTGRES_DSN="postgresql://postgres:postgres@github_postgres:5432/postgres" \
-e REDIS_HOST="redis-container" \
-e CONTAINER_ID=github-app-1
--memory 128m \
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