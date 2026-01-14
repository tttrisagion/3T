# Calculate timestamp for 12 hours ago in microseconds (Jaeger default format)
EPOCH_MICROS=$(($(date -d "12 hours ago" +%s) * 1000000))

curl -X POST "http://localhost:9200/jaeger-*/_delete_by_query?conflicts=proceed" \
     -H 'Content-Type: application/json' \
     -d '{
           "query": {
             "range": {
               "startTime": {
                 "lt": '$EPOCH_MICROS'
               }
             }
           }
         }'
