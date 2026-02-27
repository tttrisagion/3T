document.addEventListener('DOMContentLoaded', function() {
    loadAndRenderCompose();
    loadAndRenderConfig();
    setInterval(loadAndRenderCompose, 5000);
    setInterval(loadAndRenderConfig, 5000);
});

const WEB_SERVICES = [
    'mission-control', 'grafana', 'flower', 'prometheus',
    'tempo', 'exchange-observer', 'order-gateway'
];

const ICON_MAP = {
    'mariadb': 'mysql',
    'celery_worker': 'celery',
    'celery_beat': 'celery',
    'otel-collector': 'opentelemetry',
    'mission-control': 'nginx',
    'cors-proxy': 'nginx',
    'node-exporter': 'prometheus',
    'redis': 'redis',
    'tempo': 'grafana',
    'grafana': 'grafana',
    'prometheus': 'prometheus',
    'flower': 'celery',
    // Use default for custom services
    'balance_consumer': 'default',
    'take-profit': 'default',
    'price_stream_producer': 'default',
    'price_stream_consumer': 'default',
    'health-monitor': 'default',
    'order-gateway': 'default',
    'exchange-observer': 'default'
};

async function loadAndRenderConfig() {
    const container = document.getElementById('config-table-container');
    try {
        const [configResponse, descResponse] = await Promise.all([
            fetch('config.yml'),
            fetch('assets/config_descriptions.json')
        ]);

        if (!configResponse.ok) throw new Error(`Failed to fetch config.yml`);
        const yamlText = await configResponse.text();
        const config = jsyaml.load(yamlText);
        
        const descriptions = descResponse.ok ? await descResponse.json() : {};

        let table = container.querySelector('table');
        if (!table) {
            table = document.createElement('table');
            container.innerHTML = '';
            const header = table.createTHead();
            header.insertRow().innerHTML = '<th>Setting</th><th>Value</th><th>Description</th>';
            container.appendChild(table);
        }

        const tbody = table.querySelector('tbody') || table.appendChild(document.createElement('tbody'));
        const existingRows = new Map([...tbody.querySelectorAll('tr')].map(row => [row.dataset.key, row]));

        function processRows(obj, prefix = '') {
            for (const key in obj) {
                const newPrefix = prefix ? `${prefix}.${key}` : key;
                const value = obj[key];

                if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                    processRows(value, newPrefix);
                } else {
                    let row = existingRows.get(newPrefix) || tbody.insertRow();
                    row.dataset.key = newPrefix;

                    const keyCell = row.cells[0] || row.insertCell();
                    const valueCell = row.cells[1] || row.insertCell();
                    const descCell = row.cells[2] || row.insertCell();

                    if (keyCell.textContent !== newPrefix) keyCell.textContent = newPrefix;

                    let valueHTML = Array.isArray(value) ? `<ul>${value.map(v => `<li>${v}</li>`).join('')}</ul>` : String(value);
                    if (valueCell.innerHTML !== valueHTML) valueCell.innerHTML = valueHTML;

                    const descText = descriptions[newPrefix] || '';
                    if (descCell.textContent !== descText) descCell.textContent = descText;
                    
                    if (!row.parentElement) tbody.appendChild(row);
                    existingRows.delete(newPrefix);
                }
            }
        }

        processRows(config);
        existingRows.forEach(row => row.remove());

    } catch (error) {
        console.error('Error processing config.yml:', error);
        container.innerHTML = '<p>Error loading config.yml. See console for details.</p>';
    }
}

async function loadAndRenderCompose() {
    const container = document.getElementById('compose-table-container');
    try {
        const response = await fetch('docker-compose.yml');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const yamlText = await response.text();
        const composeConfig = jsyaml.load(yamlText);

        let table = container.querySelector('table');
        if (!table) {
            table = document.createElement('table');
            container.innerHTML = '';
            const header = table.createTHead();
            header.insertRow().innerHTML = '<th>Service</th><th>Image / Build Context</th><th>Ports</th>';
            container.appendChild(table);
        }

        const tbody = table.querySelector('tbody') || table.appendChild(document.createElement('tbody'));
        const services = composeConfig.services || {};
        const existingRows = new Map([...tbody.querySelectorAll('tr')].map(row => [row.dataset.service, row]));

        for (const serviceName in services) {
            const service = services[serviceName];
            let row = existingRows.get(serviceName) || tbody.insertRow();
            row.dataset.service = serviceName;

            const nameCell = row.cells[0] || row.insertCell();
            const imageCell = row.cells[1] || row.insertCell();
            const portsCell = row.cells[2] || row.insertCell();

            // Service Name with Icon
            const iconSlug = ICON_MAP[serviceName] || serviceName.split('-')[0].split('_')[0];
            const iconSrc = (iconSlug === 'default') 
                ? 'assets/logo.svg' 
                : `https://cdn.simpleicons.org/${iconSlug}/ff9900`;
            const iconHTML = `<img src="${iconSrc}" alt="" width="32" height="32" style="vertical-align: middle; margin-right: 8px;" onerror="this.style.display='none'">`;
            const finalNameHTML = `${iconHTML}${serviceName}`;
            if (nameCell.innerHTML !== finalNameHTML) nameCell.innerHTML = finalNameHTML;
            nameCell.className = 'service-name';

            // Image / Build
            let buildInfo = service.image || service.build;
            if (typeof buildInfo === 'object' && buildInfo !== null) {
                buildInfo = buildInfo.context || JSON.stringify(buildInfo);
            }
            if (buildInfo === '.') {
                buildInfo = 'N/A (Root Context)';
            }
            if (imageCell.textContent !== buildInfo) imageCell.textContent = buildInfo;

            // Ports
            let portText = service.ports ? service.ports.join(', ') : 'N/A';
            if (portsCell.textContent !== portText) portsCell.textContent = portText;
            
            // Clickable Row
            row.classList.remove('clickable-row');
            row.onclick = null;
            if (service.ports && WEB_SERVICES.includes(serviceName)) {
                const hostPort = service.ports[0].split(':')[0];
                let url = `http://${window.location.hostname}:${hostPort}`;
                if (serviceName === 'grafana') {
                    url += '/dashboards';
                }
                row.classList.add('clickable-row');
                row.onclick = () => window.open(url, '_blank');
            }

            if (!row.parentElement) tbody.appendChild(row);
            existingRows.delete(serviceName);
        }

        existingRows.forEach(row => row.remove());

    } catch (error) {
        console.error('Error processing docker-compose.yml:', error);
        container.innerHTML = '<p>Error loading docker-compose.yml. See console for details.</p>';
    }
}
