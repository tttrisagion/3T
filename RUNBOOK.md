# Weekly

## 3T Trading Infrastructure

```
# Purge Jaeger trace data (Badger backend):
# docker volume rm 3t_jaeger_data  (requires stopping jaeger first)
# Or set BADGER_SPAN_STORE_TTL env var for automatic TTL-based cleanup

cd /opt/3T
docker compose exec mariadb /bin/bash
```

```
mysql -u root -psecret 3t
delete from runs where height is null;
update take_profit_state set last_balance = (select account_value from balance_history order by timestamp desc limit 1);
```

`docker compose restart take-profit`

# 3T Strategy

```
cd /opt/3T-protected
source bin/activate
./3T.sh
```
