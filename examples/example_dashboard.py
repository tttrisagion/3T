from flask import Flask, jsonify, render_template_string
import mysql.connector
import logging
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
DB_CONFIG = {
    'user': 'root',         # CHANGE THIS
    'password': 'secret',   # CHANGE THIS
    'host': '192.168.2.93', # CHANGE THIS
    'database': '3t',       # CHANGE THIS
    'raise_on_warnings': True
}

app = Flask(__name__)
# Suppress standard flask logging to keep terminal clean
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ==========================================
# FRONTEND TEMPLATE (HTML + JS)
# ==========================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Live Efficiency Monitor</title>
    <!-- Chart.js CDN -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <!-- Date Adapter for Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <style>
        body { 
            font-family: 'Courier New', Courier, monospace; 
            background-color: #000000; 
            color: #FFA500; 
            margin: 0; 
            padding: 20px; 
            overflow: hidden; /* Prevent scrollbars */
        }
        
        h1 { 
            text-align: center; 
            margin: 0 0 10px 0; 
            color: #FFA500; 
            text-transform: uppercase;
            font-size: 1.5rem;
            border-bottom: 1px solid #333;
            padding-bottom: 10px;
        }

        .chart-container { 
            position: relative; 
            height: 85vh; 
            width: 98vw; 
            margin: 0 auto;
            border: 1px solid #333;
            background-color: #050505;
        }

        .status { 
            text-align: right; 
            margin-top: 5px; 
            font-size: 0.8em; 
            color: #cc8400; 
        }
    </style>
</head>
<body>
    <h1>System Efficiency Monitor [LIVE]</h1>
    
    <div class="chart-container">
        <canvas id="efficiencyChart"></canvas>
    </div>
    
    <div class="status" id="lastUpdated">Initializing feed...</div>

    <script>
        const ctx = document.getElementById('efficiencyChart').getContext('2d');
        
        // Configuration for the Chart
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [] // Datasets will be created dynamically based on host names
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false, // Disable animation for "terminal" feel
                layout: {
                    padding: 10
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'minute',
                            displayFormats: { minute: 'HH:mm:ss' }
                        },
                        grid: { color: '#222', borderColor: '#FFA500' },
                        ticks: { color: '#FFA500', font: { family: 'Courier New' } }
                    },
                    y: {
                        grid: { color: '#222', borderColor: '#FFA500' },
                        ticks: { color: '#FFA500', font: { family: 'Courier New' } },
                        title: { 
                            display: true, 
                            text: 'AVG (Pos / PnL)', 
                            color: '#FFA500',
                            font: { family: 'Courier New' }
                        }
                    }
                },
                plugins: {
                    legend: { 
                        labels: { 
                            color: '#FFA500', 
                            font: { family: 'Courier New', size: 14 } 
                        } 
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(0,0,0,0.9)',
                        titleColor: '#FFA500',
                        bodyColor: '#fff',
                        borderColor: '#FFA500',
                        borderWidth: 1,
                        titleFont: { family: 'Courier New' },
                        bodyFont: { family: 'Courier New' }
                    }
                }
            }
        });

        const hostColors = {
            'flippycoin': '#FFA500',        // Orange
            '0xc9d92c3e6a6': '#00FF00',     // Green terminal style for contrast
            'default': '#00FFFF'            // Cyan fallback
        };

        async function fetchData() {
            try {
                const response = await fetch('/data.json');
                const data = await response.json();
                
                if (data.error) {
                    console.error(data.error);
                    return;
                }

                const now = new Date();

                // Process each host returned by the SQL query
                data.results.forEach(row => {
                    const hostName = row.host;
                    const value = row.value;

                    // Find existing dataset for this host
                    let dataset = chart.data.datasets.find(ds => ds.label === hostName);

                    // If not found, create new dataset
                    if (!dataset) {
                        const color = hostColors[hostName] || hostColors['default'];
                        dataset = {
                            label: hostName,
                            data: [],
                            borderColor: color,
                            backgroundColor: color,
                            borderWidth: 2,
                            pointRadius: 2,
                            pointHoverRadius: 5,
                            tension: 0.1, // Slight curve, mostly straight lines
                            fill: false
                        };
                        chart.data.datasets.push(dataset);
                    }

                    // Append new data point
                    dataset.data.push({ x: now, y: value });
                });

                chart.update('none'); // Update without animation
                
                document.getElementById('lastUpdated').innerText = 'LAST SYNC: ' + now.toLocaleTimeString();
                
            } catch (err) {
                console.error("Failed to fetch data", err);
                document.getElementById('lastUpdated').innerText = 'CONNECTION ERROR';
            }
        }

        // Fetch immediately, then every 30s
        fetchData();
        setInterval(fetchData, 30000);
    </script>
</body>
</html>
"""

# ==========================================
# BACKEND LOGIC
# ==========================================
def get_data_from_db():
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # EXACT QUERY REQUESTED BY USER
        query = """
        SELECT
    host,
    run_count,
    calculated_avg_value,
    (calculated_avg_value / run_count) as final_ratio
FROM (
    SELECT
        host,
        COUNT(*) as run_count,
        AVG(
            CASE
                WHEN host = 'flippycoin' THEN (ABS(position_direction) * 10.0) / live_pnl
                WHEN host = '0xc9d92c3e6a6' THEN (ABS(position_direction) * 10) / live_pnl
                ELSE NULL
            END
        ) as calculated_avg_value
    FROM runs
    WHERE host IN ('flippycoin', '0xc9d92c3e6a6')
      AND exit_run = 0
      AND height IS NULL
      AND live_pnl > 0.2
    GROUP BY host
) as sub;
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Transform for JSON response
        results = []
        for row in rows:
            results.append({
                'host': row['host'],
                'value': float(row['final_ratio']) if row['final_ratio'] is not None else 0.0,
                'count': row['run_count']
            })
            
        return {"results": results}

    except Exception as e:
        print(f"DB Error: {e}")
        return {"error": str(e)}
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ==========================================
# ROUTES
# ==========================================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/data.json')
def data():
    json_data = get_data_from_db()
    return jsonify(json_data)

if __name__ == '__main__':
    print(f"Starting Dashboard on http://localhost:8080")
    app.run(host='0.0.0.0', port=8080, debug=True)
