/**
 * Unified Dashboard JavaScript
 * Handles all interactions for the Schema Translator unified interface
 */

// ============================================
// TAB SWITCHING
// ============================================

function switchTab(tabName) {
    // Hide all tab contents
    const allTabs = document.querySelectorAll('.tab-content');
    allTabs.forEach(tab => tab.classList.remove('active'));
    
    // Remove active class from all nav tabs
    const allNavTabs = document.querySelectorAll('.nav-tab');
    allNavTabs.forEach(tab => tab.classList.remove('active'));
    
    // Show selected tab
    const selectedTab = document.getElementById(`${tabName}-tab`);
    if (selectedTab) {
        selectedTab.classList.add('active');
    }
    
    // Activate the nav tab
    event.target.closest('.nav-tab').classList.add('active');
    
    // Load data for specific tabs
    if (tabName === 'tenants') {
        loadTenants();
    } else if (tabName === 'system') {
        loadSystemStats();
    }
}

// ============================================
// QUERY TRANSLATION
// ============================================

async function translateQuery() {
    const tenantId = document.getElementById('tenant-select').value;
    const canonicalQuery = document.getElementById('canonical-query').value;
    
    if (!tenantId) {
        showAlert('error', 'Please select a tenant');
        return;
    }
    
    if (!canonicalQuery.trim()) {
        showAlert('error', 'Please enter a canonical SQL query');
        return;
    }
    
    // Show loading state
    const translateBtn = document.getElementById('translate-btn');
    const originalHtml = '<i class="fas fa-exchange-alt"></i> Translate Query';
    translateBtn.disabled = true;
    translateBtn.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"><span class="visually-hidden">Loading...</span></div> Translating...';
    
    // Show progress container
    showProgressSteps();
    
    try {
        // Use streaming endpoint for real-time progress
        const response = await fetch('/api/query_translation/translate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'
            },
            body: JSON.stringify({
                canonical_query: canonicalQuery,
                customer_id: tenantId
            })
        });
        
        // Check if server supports streaming
        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('text/event-stream')) {
            // Handle SSE streaming
            await handleTranslationStream(response, tenantId);
        } else {
            // Fallback to regular JSON response
            const data = await response.json();
            if (data.success) {
                displayTranslationResults(data);
                // Refresh cache stats after translation
                if (typeof loadSystemStats === 'function') {
                    loadSystemStats();
                }
            } else {
                showAlert('error', data.error || 'Translation failed');
            }
        }
    } catch (error) {
        console.error('Translation error:', error);
        showAlert('error', 'Failed to translate query. Please try again.');
        hideProgressSteps();
    } finally {
        // Always reset button state
        translateBtn.disabled = false;
        translateBtn.innerHTML = originalHtml;
    }
}

async function handleTranslationStream(response, tenantId) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    
    const processStream = async ({done, value}) => {
        if (done) {
            hideProgressSteps();
            return;
        }
        
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\n');
        buffer = lines.pop(); // Keep incomplete line
        
        for (const line of lines) {
            if (line.startsWith('data: ')) {
                try {
                    const data = JSON.parse(line.slice(6));
                    handleStreamEvent(data, tenantId);
                } catch (e) {
                    console.error('Failed to parse SSE data:', e);
                }
            }
        }
        
        return reader.read().then(processStream);
    };
    
    return reader.read().then(processStream);
}

function showProgressSteps() {
    // Show or create progress container
    let progressContainer = document.getElementById('translation-progress');
    if (!progressContainer) {
        progressContainer = document.createElement('div');
        progressContainer.id = 'translation-progress';
        progressContainer.className = 'progress-steps-container';
        progressContainer.innerHTML = '<div id="progress-steps-list"></div>';
        
        // Insert before results section
        const resultsSection = document.getElementById('translation-results');
        if (resultsSection) {
            resultsSection.parentNode.insertBefore(progressContainer, resultsSection);
        }
    }
    progressContainer.style.display = 'block';
    document.getElementById('progress-steps-list').innerHTML = '';
}

function hideProgressSteps() {
    setTimeout(() => {
        const progressContainer = document.getElementById('translation-progress');
        if (progressContainer) {
            progressContainer.style.display = 'none';
        }
    }, 2000); // Keep visible for 2s after completion
}

function addProgressStep(icon, message, status = 'active') {
    const stepsList = document.getElementById('progress-steps-list');
    if (!stepsList) return;
    
    const stepDiv = document.createElement('div');
    stepDiv.className = `progress-step ${status}`;
    stepDiv.innerHTML = `
        <span class="step-icon">${icon}</span>
        <span class="step-message">${message}</span>
    `;
    stepsList.appendChild(stepDiv);
    
    // Scroll into view
    stepDiv.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function updateLastProgressStep(icon, message, status = 'complete') {
    const stepsList = document.getElementById('progress-steps-list');
    if (!stepsList || !stepsList.lastChild) return;
    
    const lastStep = stepsList.lastChild;
    lastStep.className = `progress-step ${status}`;
    lastStep.innerHTML = `
        <span class="step-icon">${icon}</span>
        <span class="step-message">${message}</span>
    `;
}

function handleStreamEvent(event, tenantId) {
    console.log('Stream event:', event);
    
    switch(event.step) {
        case 'schema_load':
            addProgressStep('⏳', event.message, 'active');
            break;
            
        case 'schema_loaded':
            updateLastProgressStep('✅', event.message, 'complete');
            break;
            
        case 'cache_check':
            addProgressStep('⏳', event.message, 'active');
            break;
            
        case 'cache_result':
            updateLastProgressStep('✅', event.message, 'complete');
            break;
            
        case 'mapping_discovery':
            addProgressStep('⏳', event.message, 'active');
            break;
            
        case 'mapping_complete':
            updateLastProgressStep('✅', event.message, 'complete');
            break;
            
        case 'translation_start':
            addProgressStep('⏳', event.message, 'active');
            break;
            
        case 'translation_complete':
            updateLastProgressStep('✅', event.message, 'complete');
            if (event.data) {
                displayTranslationResults({
                    success: true,
                    translated_query: event.data.sql,
                    confidence: event.data.confidence,
                    warnings: event.data.warnings || [],
                    validation_errors: event.data.validation_errors || []
                });
            }
            break;
            
        case 'complete':
            addProgressStep('🎉', event.message, 'complete');
            if (event.data && event.data.translated_query) {
                displayTranslationResults({
                    success: true,
                    translated_query: event.data.translated_query,
                    confidence: event.data.confidence,
                    warnings: event.data.warnings || [],
                    validation_errors: event.data.validation_errors || [],
                    reasoning: event.data.reasoning,
                    cache_hit: event.data.cache_hit
                });
            }
            hideProgressSteps();
            
            // Refresh cache stats after translation
            if (typeof loadSystemStats === 'function') {
                loadSystemStats();
            }
            break;
            
        case 'error':
            updateLastProgressStep('❌', event.message, 'error');
            showAlert('error', event.data?.error || 'An error occurred');
            hideProgressSteps();
            break;
    }
}

function displayTranslationResults(data) {
    const resultsSection = document.getElementById('translation-results');
    resultsSection.style.display = 'block';
    
    // Display translated query
    document.getElementById('translated-query-code').textContent = data.translated_query;
    
    // Display confidence
    const confidence = (data.confidence * 100).toFixed(1);
    const confidenceBadge = document.getElementById('confidence-badge');
    confidenceBadge.textContent = `Confidence: ${confidence}%`;
    confidenceBadge.className = `badge ${confidence > 80 ? 'badge-success' : confidence > 60 ? 'badge-warning' : 'badge-danger'}`;
    
    // Display validation status
    const validationBadge = document.getElementById('validation-badge');
    if (data.validation_errors && data.validation_errors.length > 0) {
        validationBadge.textContent = 'Validation: Failed';
        validationBadge.className = 'badge badge-danger';
    } else {
        validationBadge.textContent = 'Validation: Passed';
        validationBadge.className = 'badge badge-success';
    }
    
    // Display metadata
    document.getElementById('translation-path').textContent = data.translation_path || 'Complex (LLM)';
    document.getElementById('cache-status').textContent = data.cache_hit ? 'Hit ⚡' : 'Miss';
    document.getElementById('translation-time').textContent = data.translation_time ? `${data.translation_time}ms` : '-';
    
    // Display warnings
    if (data.warnings && data.warnings.length > 0) {
        const warningsSection = document.getElementById('warnings-section');
        const warningsList = document.getElementById('warnings-list');
        warningsSection.style.display = 'block';
        warningsList.innerHTML = data.warnings.map(w => `<li>${w}</li>`).join('');
    } else {
        document.getElementById('warnings-section').style.display = 'none';
    }
    
    // Display errors
    if (data.validation_errors && data.validation_errors.length > 0) {
        const errorsSection = document.getElementById('errors-section');
        const errorsList = document.getElementById('errors-list');
        errorsSection.style.display = 'block';
        errorsList.innerHTML = data.validation_errors.map(e => `<li>${e}</li>`).join('');
    } else {
        document.getElementById('errors-section').style.display = 'none';
    }
    
    // Scroll to results
    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function loadExample() {
    document.getElementById('tenant-select').value = 'tenant_A';
    document.getElementById('canonical-query').value = `SELECT 
    contract_id,
    status,
    value_amount,
    party_name
FROM contracts c
INNER JOIN parties p ON c.buyer_party_id = p.party_id
WHERE status = 'active' 
  AND value_amount > 100000
ORDER BY value_amount DESC
LIMIT 10`;
}

function clearQuery() {
    document.getElementById('canonical-query').value = '';
    document.getElementById('translation-results').style.display = 'none';
}

// ============================================
// NATURAL LANGUAGE QUERY
// ============================================

async function translateNLQuery() {
    const tenantId = document.getElementById('nl-tenant-select').value;
    const nlQuery = document.getElementById('nl-query').value;
    
    if (!tenantId) {
        showAlert('error', 'Please select a tenant');
        return;
    }
    
    if (!nlQuery.trim()) {
        showAlert('error', 'Please enter your question');
        return;
    }
    
    // Show loading state
    const translateBtn = document.getElementById('nl-translate-btn');
    const originalText = translateBtn.innerHTML;
    translateBtn.disabled = true;
    translateBtn.innerHTML = '<span class="spinner spinner-sm"></span> Generating SQL...';
    
    try {
        const response = await fetch('/api/nl-to-sql/translate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                natural_language_query: nlQuery,
                tenant_id: tenantId
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayNLResults(data);
        } else {
            showAlert('error', data.error || 'SQL generation failed');
        }
    } catch (error) {
        console.error('NL translation error:', error);
        showAlert('error', 'Failed to generate SQL. Please try again.');
    } finally {
        translateBtn.disabled = false;
        translateBtn.innerHTML = originalText;
    }
}

async function translateNLQueryAndExecute() {
    const tenantId = document.getElementById('nl-tenant-select').value;
    const nlQuery = document.getElementById('nl-query').value;
    
    if (!tenantId) {
        showAlert('error', 'Please select a tenant');
        return;
    }
    
    if (!nlQuery.trim()) {
        showAlert('error', 'Please enter your question');
        return;
    }
    
    // Show loading state
    const executeBtn = document.getElementById('nl-execute-btn');
    if (executeBtn) {
        const originalText = executeBtn.innerHTML;
        executeBtn.disabled = true;
        executeBtn.innerHTML = '<span class="spinner spinner-sm"></span> Processing...';
        
        try {
            const response = await fetch('/api/nl-to-sql/translate-and-execute', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    natural_language_query: nlQuery,
                    tenant_id: tenantId
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                displayNLExecutionResults(data);
                showAlert('success', `Query executed successfully! ${data.stages.execution.row_count || 0} rows returned.`);
            } else {
                showAlert('error', data.error || 'Execution failed');
                if (data.hint) {
                    showAlert('info', data.hint);
                }
            }
        } catch (error) {
            console.error('NL execution error:', error);
            showAlert('error', 'Failed to execute query. Please try again.');
        } finally {
            executeBtn.disabled = false;
            executeBtn.innerHTML = originalText;
        }
    }
}

function displayNLExecutionResults(data) {
    const resultsSection = document.getElementById('nl-results');
    resultsSection.style.display = 'block';
    
    const stages = data.stages;
    
    // Build comprehensive results HTML
    let html = `
        <!-- Natural Language Query -->
        <div class="card mb-3">
            <div class="card-header bg-info text-white">
                <h6 class="mb-0"><i class="fas fa-comments"></i> 1. Natural Language Query</h6>
            </div>
            <div class="card-body">
                <p class="mb-2"><strong>Your Question:</strong></p>
                <div class="alert alert-light">${stages.natural_language.query}</div>
                <div class="row mt-3">
                    <div class="col-md-4">
                        <small class="text-muted">Intent:</small>
                        <div><span class="badge bg-primary">${stages.natural_language.intent}</span></div>
                    </div>
                    <div class="col-md-4">
                        <small class="text-muted">Entity:</small>
                        <div><span class="badge bg-secondary">${stages.natural_language.primary_entity}</span></div>
                    </div>
                    <div class="col-md-4">
                        <small class="text-muted">Filters:</small>
                        <div><span class="badge bg-info">${stages.natural_language.filter_conditions.length} conditions</span></div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Canonical SQL -->
        <div class="card mb-3">
            <div class="card-header bg-primary text-white">
                <div class="d-flex justify-content-between align-items-center">
                    <h6 class="mb-0"><i class="fas fa-code"></i> 2. Canonical SQL (Standard)</h6>
                    <button class="btn btn-sm btn-light" onclick="copyToClipboard('canonical-sql-code')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
            </div>
            <div class="card-body">
                <pre class="mb-0"><code id="canonical-sql-code" class="language-sql">${stages.canonical_sql.query}</code></pre>
                <div class="mt-3">
                    <span class="badge bg-success">Confidence: ${(stages.canonical_sql.confidence * 100).toFixed(1)}%</span>
                    <span class="badge bg-info ms-2">Tables: ${stages.canonical_sql.tables_used.join(', ')}</span>
                </div>
            </div>
        </div>
        
        <!-- Tenant-Specific SQL -->
        <div class="card mb-3">
            <div class="card-header bg-success text-white">
                <div class="d-flex justify-content-between align-items-center">
                    <h6 class="mb-0"><i class="fas fa-database"></i> 3. Tenant SQL (${stages.tenant_sql.tenant_id})</h6>
                    <button class="btn btn-sm btn-light" onclick="copyToClipboard('tenant-sql-code')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
            </div>
            <div class="card-body">
                <pre class="mb-0"><code id="tenant-sql-code" class="language-sql">${stages.tenant_sql.query}</code></pre>
                <div class="mt-3">
                    <span class="badge bg-success">Translation Confidence: ${(stages.tenant_sql.confidence * 100).toFixed(1)}%</span>
                </div>
            </div>
        </div>
        
        <!-- Query Results -->
        <div class="card">
            <div class="card-header bg-dark text-white">
                <h6 class="mb-0">
                    <i class="fas fa-table"></i> 4. Query Results
                    <span class="badge bg-light text-dark ms-2">${stages.execution.row_count || 0} rows</span>
                </h6>
            </div>
            <div class="card-body">
    `;
    
    if (stages.execution.success && stages.execution.rows && stages.execution.rows.length > 0) {
        html += `
                <div class="table-responsive">
                    <table class="table table-striped table-hover table-sm">
                        <thead class="table-dark">
                            <tr>
        `;
        
        // Add column headers
        stages.execution.columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        
        html += `
                            </tr>
                        </thead>
                        <tbody>
        `;
        
        // Add rows
        stages.execution.rows.forEach(row => {
            html += '<tr>';
            stages.execution.columns.forEach(col => {
                const value = row[col];
                const displayValue = value === null || value === undefined 
                    ? '<em class="text-muted">NULL</em>' 
                    : value;
                html += `<td>${displayValue}</td>`;
            });
            html += '</tr>';
        });
        
        html += `
                        </tbody>
                    </table>
                </div>
        `;
    } else if (!stages.execution.success && stages.execution.error) {
        // Display error message
        html += `
                <div class="alert alert-danger">
                    <div class="d-flex align-items-start">
                        <i class="fas fa-exclamation-circle me-2 mt-1"></i>
                        <div>
                            <strong>Query Execution Failed</strong>
                            <p class="mb-0 mt-2">${stages.execution.error}</p>
                            ${stages.execution.error.includes('array_remove') || stages.execution.error.includes('ARRAY_REMOVE') ? 
                                '<p class="mb-0 mt-2"><small><strong>Tip:</strong> This is a DuckDB compatibility issue. The LLM generated SQL with a PostgreSQL function. Try a simpler query like "Show me 10 awards" without complex array operations.</small></p>' 
                                : ''}
                        </div>
                    </div>
                </div>
        `;
    } else {
        html += `<div class="alert alert-warning">No results returned</div>`;
    }
    
    html += `
            </div>
        </div>
    `;
    
    resultsSection.innerHTML = html;
    
    // Scroll to results
    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function displayNLResults(data) {
    const resultsSection = document.getElementById('nl-results');
    resultsSection.style.display = 'block';
    
    // Display generated SQL (try different field names for compatibility)
    const sqlQuery = data.translated_query || data.canonical_query || data.final_query || data.sql_query || 'No query generated';
    document.getElementById('nl-query-code').textContent = sqlQuery;
    
    // Show translation error if present
    if (data.translation_error) {
        showAlert('warning', `Note: ${data.translation_error}. Showing canonical SQL instead.`);
    }
    
    // Scroll to results
    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ============================================
// TENANTS MANAGEMENT
// ============================================

async function loadTenants() {
    try {
        const response = await fetch('/api/tenants/list');
        const data = await response.json();
        
        const tenantsGrid = document.getElementById('tenants-grid');
        tenantsGrid.innerHTML = '';
        
        const tenantInfo = {
            'tenant_A': { name: 'USAspending', desc: 'Federal Awards Database', tables: 5, color: 'primary' },
            'tenant_B': { name: 'World Bank', desc: 'Projects Database', tables: 4, color: 'success' },
            'tenant_C': { name: 'OCDS', desc: 'Procurement Standards', tables: 6, color: 'info' },
            'tenant_D': { name: 'Enterprise', desc: 'Enterprise Contracts', tables: 6, color: 'warning' },
            'tenant_E': { name: 'Government', desc: 'Government Contracts', tables: 3, color: 'danger' }
        };
        
        Object.keys(tenantInfo).forEach(tenantId => {
            const info = tenantInfo[tenantId];
            const card = `
                <div class="card">
                    <div class="card-body">
                        <div class="d-flex justify-content-between align-items-start mb-3">
                            <div>
                                <h6 class="fw-bold mb-1">${info.name}</h6>
                                <p class="text-muted mb-0" style="font-size: 0.875rem;">${info.desc}</p>
                            </div>
                            <span class="badge badge-${info.color}">${tenantId.toUpperCase()}</span>
                        </div>
                        <div class="d-flex justify-content-between text-muted mb-3" style="font-size: 0.875rem;">
                            <span><i class="fas fa-table"></i> ${info.tables} Tables</span>
                            <span><i class="fas fa-check-circle text-success"></i> Schema Ready</span>
                        </div>
                        <button class="btn btn-sm btn-outline-primary w-100" onclick="toggleTenantSchema('${tenantId}')">
                            <i class="fas fa-eye"></i> View Tables & Columns
                        </button>
                        <div id="schema-${tenantId}" class="mt-3" style="display: none;">
                            <div class="text-center py-3">
                                <i class="fas fa-spinner fa-spin"></i> Loading schema...
                            </div>
                        </div>
                    </div>
                </div>
            `;
            tenantsGrid.innerHTML += card;
        });
    } catch (error) {
        console.error('Error loading tenants:', error);
        showAlert('error', 'Failed to load tenants');
    }
}

async function toggleTenantSchema(tenantId) {
    const schemaDiv = document.getElementById(`schema-${tenantId}`);
    
    if (schemaDiv.style.display === 'none') {
        schemaDiv.style.display = 'block';
        
        // Load schema if not already loaded
        if (!schemaDiv.dataset.loaded) {
            try {
                const response = await fetch(`/api/tenants/${tenantId}/schema`);
                const data = await response.json();
                
                if (data.success) {
                    let schemaHTML = '';
                    data.tables.forEach(table => {
                        schemaHTML += `
                            <div class="mb-3">
                                <div class="d-flex justify-content-between align-items-center p-2" style="background: #f8f9fa; border-radius: 4px; cursor: pointer;" onclick="toggleTable('${tenantId}-${table.name}')">
                                    <div>
                                        <strong><i class="fas fa-table"></i> ${table.name}</strong>
                                        <small class="text-muted d-block">${table.description || 'No description'}</small>
                                    </div>
                                    <span class="badge badge-secondary">${table.column_count} columns</span>
                                </div>
                                <div id="table-${tenantId}-${table.name}" class="mt-2" style="display: none;">
                                    <table class="table table-sm table-bordered">
                                        <thead style="background: #e9ecef;">
                                            <tr>
                                                <th>Column</th>
                                                <th>Type</th>
                                                <th>Description</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${table.columns.map(col => `
                                                <tr>
                                                    <td><code>${col.name}</code></td>
                                                    <td><small class="text-muted">${col.type}</small></td>
                                                    <td><small>${col.description || '-'}</small></td>
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        `;
                    });
                    schemaDiv.innerHTML = schemaHTML;
                    schemaDiv.dataset.loaded = 'true';
                } else {
                    schemaDiv.innerHTML = `<div class="alert alert-danger">${data.error}</div>`;
                }
            } catch (error) {
                schemaDiv.innerHTML = `<div class="alert alert-danger">Failed to load schema: ${error.message}</div>`;
            }
        }
    } else {
        schemaDiv.style.display = 'none';
    }
}

function toggleTable(tableId) {
    const tableDiv = document.getElementById(`table-${tableId}`);
    if (tableDiv.style.display === 'none') {
        tableDiv.style.display = 'block';
    } else {
        tableDiv.style.display = 'none';
    }
}

// ============================================
// SYSTEM STATS
// ============================================

async function loadSystemStats() {
    try {
        // Load cache stats
        const cacheResponse = await fetch('/api/cache/stats');
        const cacheData = await cacheResponse.json();
        
        if (cacheData.success) {
            // Update overall stats
            document.getElementById('cached-tenants').textContent = cacheData.stats.cached_tenants || 0;
            document.getElementById('cache-hits').textContent = cacheData.stats.cache_hits || 0;
            document.getElementById('cache-misses').textContent = cacheData.stats.cache_misses || 0;
            
            const hitRate = cacheData.stats.hit_rate || 0;
            document.getElementById('cache-hit-rate').textContent = `${hitRate.toFixed(1)}%`;
            
            // Update average translation time
            const avgTime = cacheData.stats.avg_translation_time || 0.4;
            const avgTimeElement = document.getElementById('avg-translation-time');
            if (avgTimeElement) {
                avgTimeElement.textContent = `${avgTime.toFixed(2)}s`;
            }
            
            // Update per-tenant cache table
            updateTenantCacheTable(cacheData.tenant_details || []);
        }
    } catch (error) {
        console.error('Error loading system stats:', error);
    }
}

function updateTenantCacheTable(tenantDetails) {
    const tableBody = document.getElementById('tenant-cache-table');
    
    if (!tenantDetails || tenantDetails.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No tenants found</td></tr>';
        return;
    }
    
    tableBody.innerHTML = tenantDetails.map(tenant => {
        const statusBadge = tenant.is_cached 
            ? '<span class="badge bg-success"><i class="fas fa-check-circle"></i> Cached</span>'
            : '<span class="badge bg-secondary"><i class="fas fa-snowflake"></i> Cold</span>';
        
        const fieldMappings = tenant.is_cached ? tenant.field_mappings : '-';
        const complexMappings = tenant.is_cached ? tenant.complex_mappings : '-';
        const usageCount = tenant.is_cached ? tenant.usage_count : '-';
        const lastUpdated = tenant.is_cached ? formatTimestamp(tenant.last_updated) : 'Never';
        
        return `
            <tr>
                <td>
                    <div class="fw-bold">${tenant.tenant_id}</div>
                    <small class="text-muted">${tenant.tenant_name}</small>
                </td>
                <td>${statusBadge}</td>
                <td class="text-center">${fieldMappings}</td>
                <td class="text-center">${complexMappings}</td>
                <td class="text-center">${usageCount}</td>
                <td><small>${lastUpdated}</small></td>
            </tr>
        `;
    }).join('');
}

function formatTimestamp(timestamp) {
    if (!timestamp || timestamp === 'Never') return 'Never';
    try {
        const date = new Date(timestamp);
        return date.toLocaleString();
    } catch {
        return timestamp;
    }
}

// ============================================
// UTILITY FUNCTIONS
// ============================================

function copyToClipboard(elementId) {
    const element = document.getElementById(elementId);
    const text = element.textContent;
    
    navigator.clipboard.writeText(text).then(() => {
        showAlert('success', 'Copied to clipboard!');
    }).catch(err => {
        console.error('Failed to copy:', err);
        showAlert('error', 'Failed to copy to clipboard');
    });
}

function showAlert(type, message) {
    // Create alert element
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type === 'error' ? 'danger' : type}`;
    alertDiv.style.position = 'fixed';
    alertDiv.style.top = '20px';
    alertDiv.style.right = '20px';
    alertDiv.style.zIndex = '9999';
    alertDiv.style.minWidth = '300px';
    alertDiv.style.boxShadow = 'var(--shadow-lg)';
    
    const icon = type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : 'info-circle';
    
    alertDiv.innerHTML = `
        <div class="d-flex align-items-center gap-2">
            <i class="fas fa-${icon}"></i>
            <span>${message}</span>
        </div>
    `;
    
    document.body.appendChild(alertDiv);
    
    // Auto-remove after 3 seconds
    setTimeout(() => {
        alertDiv.style.opacity = '0';
        setTimeout(() => alertDiv.remove(), 300);
    }, 3000);
}

// ============================================
// INITIALIZATION
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    // Load initial data
    loadSystemStats();
    
    // Set up event listeners
    document.getElementById('translate-btn')?.addEventListener('click', translateQuery);
    document.getElementById('nl-translate-btn')?.addEventListener('click', translateNLQuery);
    
    // Allow Enter key to submit in text areas (with Ctrl/Cmd)
    document.getElementById('canonical-query')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
            translateQuery();
        }
    });
    
    document.getElementById('nl-query')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
            translateNLQuery();
        }
    });
});

// ============================================
// EXPORT FUNCTIONS FOR GLOBAL ACCESS
// ============================================

window.switchTab = switchTab;
window.translateQuery = translateQuery;
window.translateNLQuery = translateNLQuery;
// ============================================
// QUERY EXECUTION
// ============================================

async function executeQuery() {
    const tenantId = document.getElementById('tenant-select').value;
    const translatedQuery = document.getElementById('translated-query')?.textContent || '';
    
    if (!tenantId) {
        showAlert('error', 'Please select a tenant');
        return;
    }
    
    if (!translatedQuery.trim()) {
        showAlert('error', 'Please translate a query first');
        return;
    }
    
    // Show loading state
    const executeBtn = document.getElementById('execute-btn');
    if (executeBtn) {
        executeBtn.disabled = true;
        executeBtn.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Executing...';
    }
    
    try {
        const response = await fetch('/api/query-execution/execute', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                tenant_id: tenantId,
                query: translatedQuery
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            displayQueryResults(data);
            showAlert('success', `Query executed successfully! ${data.row_count} rows returned.`);
        } else {
            showAlert('error', data.error || 'Query execution failed');
            if (data.hint) {
                showAlert('info', data.hint);
            }
        }
    } catch (error) {
        console.error('Execution error:', error);
        showAlert('error', 'Failed to execute query. Please try again.');
    } finally {
        if (executeBtn) {
            executeBtn.disabled = false;
            executeBtn.innerHTML = '<i class="fas fa-play"></i> Execute Query';
        }
    }
}

async function translateAndExecute() {
    const tenantId = document.getElementById('tenant-select').value;
    const canonicalQuery = document.getElementById('canonical-query').value;
    
    if (!tenantId || !canonicalQuery.trim()) {
        showAlert('error', 'Please select a tenant and enter a query');
        return;
    }
    
    const btn = document.getElementById('translate-and-execute-btn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<div class="spinner-border spinner-border-sm"></div> Processing...';
    }
    
    try {
        const response = await fetch('/api/query-execution/translate-and-execute', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                canonical_query: canonicalQuery,
                tenant_id: tenantId
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Display translation results
            displayTranslationResults({
                success: true,
                translated_query: data.translation.translated_query,
                confidence: data.translation.confidence,
                warnings: data.translation.warnings,
                reasoning: data.translation.reasoning
            });
            
            // Display execution results
            displayQueryResults(data.execution);
            
            showAlert('success', `Query translated and executed! ${data.execution.row_count || 0} rows returned.`);
        } else {
            showAlert('error', data.error || 'Translation and execution failed');
            if (data.hint) {
                showAlert('info', data.hint);
            }
        }
    } catch (error) {
        console.error('Error:', error);
        showAlert('error', 'Failed to process query. Please try again.');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-bolt"></i> Translate & Execute';
        }
    }
}

function displayQueryResults(data) {
    const resultsContainer = document.getElementById('query-results');
    if (!resultsContainer) {
        // Create results container if it doesn't exist
        const container = document.createElement('div');
        container.id = 'query-results';
        container.className = 'mt-4';
        
        const resultsSection = document.querySelector('#translation-tab');
        if (resultsSection) {
            resultsSection.appendChild(container);
        }
    }
    
    const container = document.getElementById('query-results');
    
    if (!data.columns || !data.rows) {
        container.innerHTML = '<div class="alert alert-warning">No results to display</div>';
        return;
    }
    
    // Build HTML table
    let html = `
        <div class="card">
            <div class="card-header bg-success text-white">
                <h5 class="mb-0">
                    <i class="fas fa-table"></i> Query Results
                    <span class="badge bg-light text-dark ms-2">${data.row_count} rows</span>
                </h5>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover table-sm">
                        <thead class="table-dark">
                            <tr>
    `;
    
    // Add column headers
    data.columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    
    html += `
                            </tr>
                        </thead>
                        <tbody>
    `;
    
    // Add rows
    data.rows.forEach(row => {
        html += '<tr>';
        data.columns.forEach(col => {
            const value = row[col];
            const displayValue = value === null || value === undefined ? '<em class="text-muted">NULL</em>' : value;
            html += `<td>${displayValue}</td>`;
        });
        html += '</tr>';
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

window.loadExample = loadExample;
window.clearQuery = clearQuery;
window.copyToClipboard = copyToClipboard;
window.executeQuery = executeQuery;
window.translateAndExecute = translateAndExecute;
window.translateNLQueryAndExecute = translateNLQueryAndExecute;
