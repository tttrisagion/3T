# Weekly

## 3T Trading Infrastructure

`docker compose exec mariadb /bin/bash`

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
