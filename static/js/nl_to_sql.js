/**
 * Natural Language to SQL - Frontend JavaScript
 * Handles user interactions, animations, and API communication
 */

class NLToSQLInterface {
    constructor() {
        this.currentStep = 0;
        this.intentAnalysis = null;
        this.sqlGeneration = null;
        this.loadingSteps = [
            'step-parsing',
            'step-intent', 
            'step-mapping',
            'step-validation',
            'step-sql'
        ];
        
        this.init();
    }

    init() {
        this.bindEvents();
        this.setupAnimations();
        this.loadExamples();
    }

    bindEvents() {
        // Main action buttons
        const analyzeBtn = document.getElementById('analyze-btn');
        if (analyzeBtn) {
            analyzeBtn.addEventListener('click', () => {
                console.log('Analyze button clicked');
                this.analyzeQuestion();
            });
            console.log('Analyze button event listener attached');
        } else {
            console.error('Analyze button not found!');
        }
        document.getElementById('generate-sql-btn')?.addEventListener('click', () => this.generateSQL());
        document.getElementById('translate-btn')?.addEventListener('click', () => this.translateToCustomers());
        document.getElementById('clear-btn')?.addEventListener('click', () => this.clearAll());
        document.getElementById('refine-btn')?.addEventListener('click', () => this.refineQuestion());
        document.getElementById('new-question-btn')?.addEventListener('click', () => this.askNewQuestion());

        // Examples and help
        document.getElementById('examples-btn')?.addEventListener('click', () => this.showExamples());
        document.getElementById('examples-close')?.addEventListener('click', () => this.hideExamples());
        document.getElementById('examples-overlay')?.addEventListener('click', (e) => {
            if (e.target.id === 'examples-overlay') this.hideExamples();
        });

        // SQL actions
        document.getElementById('copy-sql-btn')?.addEventListener('click', () => this.copySQLToClipboard());
        document.getElementById('copy-btn')?.addEventListener('click', () => this.copySQLToClipboard());
        document.getElementById('edit-sql-btn')?.addEventListener('click', () => this.editSQL());

        // Popular tags
        document.querySelectorAll('.tag[data-query]').forEach(tag => {
            tag.addEventListener('click', () => {
                document.getElementById('question-input').value = tag.dataset.query;
                this.analyzeQuestion();
            });
        });

        // Example items
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('example-item')) {
                document.getElementById('question-input').value = e.target.dataset.query;
                this.hideExamples();
                setTimeout(() => this.analyzeQuestion(), 300);
            }
        });

        // Enter key to analyze
        document.getElementById('question-input')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && e.ctrlKey) {
                this.analyzeQuestion();
            }
        });

        // Auto-resize textarea
        const textarea = document.getElementById('question-input');
        if (textarea) {
            textarea.addEventListener('input', this.autoResize);
        }
    }

    setupAnimations() {
        // Intersection Observer for section animations
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    entry.target.classList.add('animate-in');
                }
            });
        }, { threshold: 0.1 });

        document.querySelectorAll('.nl-section').forEach(section => {
            observer.observe(section);
        });
    }

    loadExamples() {
        // Examples are already in the HTML template
        console.log('Examples loaded from template');
    }

    async analyzeQuestion() {
        console.log('analyzeQuestion method called');
        alert('Button clicked! Analyzing question...'); // Temporary debug alert
        const questionInput = document.getElementById('question-input');
        const question = questionInput?.value.trim();
        console.log('Question input element:', questionInput);
        console.log('Question value:', question);
        
        if (!question) {
            console.log('No question entered, showing error');
            this.showError('Please enter a question first.');
            return;
        }

        this.showLoading();
        this.hideSection('intent-section');
        this.hideSection('sql-section');
        this.hideSection('translation-section');

        try {
            const response = await fetch('/api/nl-to-sql/analyze', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ query: question })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.intentAnalysis = data.intent_analysis;
            
            this.hideLoading();
            this.displayIntentAnalysis(data.intent_analysis);
            this.showSection('intent-section');

        } catch (error) {
            console.error('Error analyzing question:', error);
            this.hideLoading();
            this.showError('Failed to analyze your question. Please try again.');
        }
    }

    async generateSQL() {
        if (!this.intentAnalysis) {
            this.showError('Please analyze your question first.');
            return;
        }

        this.showLoading();
        this.hideSection('sql-section');
        this.hideSection('translation-section');

        try {
            const response = await fetch('/api/nl-to-sql/generate-sql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ 
                    intent_analysis: this.intentAnalysis
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.sqlGeneration = data.sql_generation;
            
            this.hideLoading();
            this.displaySQLGeneration(data.sql_generation);
            this.showSection('sql-section');

        } catch (error) {
            console.error('Error generating SQL:', error);
            this.hideLoading();
            this.showError('Failed to generate SQL. Please try again.');
        }
    }

    async translateToCustomers() {
        if (!this.sqlGeneration) {
            this.showError('Please generate SQL first.');
            return;
        }

        this.showLoading();

        try {
            // Use existing query translation system
            const response = await fetch('/api/translate-query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    canonical_query: this.sqlGeneration.sql_query,
                    customers: ['tenant_A', 'tenant_B', 'tenant_C', 'tenant_D', 'tenant_E'] // Get from config
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            
            this.hideLoading();
            this.displayTranslationResults(data);
            this.showSection('translation-section');

        } catch (error) {
            console.error('Error translating to customers:', error);
            this.hideLoading();
            this.showError('Failed to translate to customer schemas. Please try again.');
        }
    }

    displayIntentAnalysis(analysis) {
        console.log('displayIntentAnalysis called with:', analysis);
        // Update confidence
        this.updateConfidence('intent-confidence', analysis.confidence);
        
        // Update summary
        const summaryText = this.buildIntentSummary(analysis);
        document.getElementById('intent-summary-text').textContent = summaryText;

        // Update primary entity
        document.getElementById('primary-entity').textContent = analysis.primary_entity;

        // Update requested fields
        const fieldsList = document.getElementById('requested-fields');
        fieldsList.innerHTML = '';
        analysis.requested_fields.forEach(field => {
            const li = document.createElement('li');
            li.textContent = field;
            fieldsList.appendChild(li);
        });

        // Update filter conditions
        const conditionsList = document.getElementById('filter-conditions');
        conditionsList.innerHTML = '';
        analysis.filter_conditions.forEach(condition => {
            const li = document.createElement('li');
            li.textContent = `${condition.field} ${condition.operator} ${condition.value}`;
            conditionsList.appendChild(li);
        });

        // Update date ranges
        const dateRanges = document.getElementById('date-ranges');
        if (analysis.date_ranges && analysis.date_ranges.length > 0) {
            const ranges = analysis.date_ranges.map(range => 
                `${range.original_text}: ${range.start_date} to ${range.end_date}`
            ).join(', ');
            dateRanges.textContent = ranges;
        } else {
            dateRanges.textContent = 'No specific date ranges';
        }

        // Show assumptions if any
        if (analysis.assumptions && analysis.assumptions.length > 0) {
            this.displayAssumptions(analysis.assumptions);
        }

        // Show clarifications if any
        if (analysis.clarifications_needed && analysis.clarifications_needed.length > 0) {
            this.displayClarifications(analysis.clarifications_needed);
        }
    }

    displaySQLGeneration(generation) {
        // Display SQL with syntax highlighting
        this.displaySQLWithHighlighting(generation.sql_query);

        // Update analysis cards
        document.getElementById('tables-count').textContent = generation.tables_used.length;
        document.getElementById('fields-count').textContent = generation.fields_used.length;
        
        // Estimate conditions count from SQL
        const conditionsCount = (generation.sql_query.match(/WHERE|AND|OR/gi) || []).length;
        document.getElementById('conditions-count').textContent = conditionsCount;

        // Set complexity
        const complexity = this.determineComplexity(generation);
        document.getElementById('complexity').textContent = complexity;

        // Show validation results
        this.displayValidationResults(generation);
    }

    displaySQLWithHighlighting(sqlQuery) {
        const sqlContent = document.getElementById('sql-content');
        const lines = sqlQuery.split('\n');
        
        sqlContent.innerHTML = '';
        lines.forEach((line, index) => {
            const lineDiv = document.createElement('div');
            lineDiv.className = 'sql-line';
            
            const lineNumber = document.createElement('span');
            lineNumber.className = 'sql-line-number';
            lineNumber.textContent = index + 1;
            
            const lineContent = document.createElement('span');
            lineContent.className = 'sql-line-content';
            lineContent.innerHTML = this.highlightSQL(line);
            
            lineDiv.appendChild(lineNumber);
            lineDiv.appendChild(lineContent);
            sqlContent.appendChild(lineDiv);
        });
    }

    highlightSQL(sql) {
        // Simple SQL syntax highlighting
        return sql
            .replace(/\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|OUTER|ON|GROUP BY|ORDER BY|HAVING|LIMIT|AS|AND|OR|NOT|IN|BETWEEN|LIKE|IS|NULL|CASE|WHEN|THEN|ELSE|END|UNION|DISTINCT|COUNT|SUM|AVG|MAX|MIN)\b/gi, '<span class="sql-keyword">$1</span>')
            .replace(/'([^']*)'/g, '<span class="sql-string">\'$1\'</span>')
            .replace(/\b(\d+)\b/g, '<span class="sql-number">$1</span>')
            .replace(/(=|>|<|>=|<=|!=|<>)/g, '<span class="sql-operator">$1</span>')
            .replace(/\b(COUNT|SUM|AVG|MAX|MIN|DATE|YEAR|MONTH|DAY)\s*\(/gi, '<span class="sql-function">$1</span>(');
    }

    displayValidationResults(generation) {
        // This would show actual validation results
        // For now, showing success for valid queries
        console.log('Validation status:', generation.validation_status);
    }

    displayTranslationResults(data) {
        const resultsDiv = document.getElementById('translation-results');
        
        // Create a simple results display
        // In a real implementation, this would integrate with the existing translation UI
        resultsDiv.innerHTML = `
            <div class="translation-summary">
                <h4>Translation Summary</h4>
                <p>Successfully translated your query for ${Object.keys(data.results || {}).length} customer schemas.</p>
                <p>You can now use these queries to get consistent results across all your customers.</p>
            </div>
        `;

        // Note: In the real implementation, this would load the existing query translation interface
    }

    buildIntentSummary(analysis) {
        const intent = analysis.query_intent.replace('_', ' ');
        const entity = analysis.primary_entity;
        
        let summary = `I understand you want to ${intent} from ${entity}`;
        
        if (analysis.filter_conditions.length > 0) {
            summary += ' where ';
            const conditions = analysis.filter_conditions.map(c => {
                if (c.operator === '=') return `${c.field} is ${c.value}`;
                if (c.operator === '>') return `${c.field} is more than ${c.value}`;
                if (c.operator === '<') return `${c.field} is less than ${c.value}`;
                return `${c.field} ${c.operator} ${c.value}`;
            });
            summary += conditions.join(' and ');
        }

        if (analysis.date_ranges.length > 0) {
            const dateRange = analysis.date_ranges[0];
            summary += ` during the period from ${dateRange.start_date} to ${dateRange.end_date}`;
        }

        return summary + '.';
    }

    displayAssumptions(assumptions) {
        const card = document.getElementById('assumptions-card');
        const content = document.getElementById('assumptions-content');
        
        content.innerHTML = '';
        assumptions.forEach(assumption => {
            const item = document.createElement('div');
            item.className = 'assumption-item';
            item.innerHTML = `
                <span class="assumption-icon">💭</span>
                <span>${assumption}</span>
            `;
            content.appendChild(item);
        });
        
        card.style.display = 'block';
    }

    displayClarifications(clarifications) {
        const card = document.getElementById('clarifications-card');
        const content = document.getElementById('clarifications-content');
        
        content.innerHTML = '';
        clarifications.forEach(clarification => {
            const item = document.createElement('div');
            item.className = 'clarification-item';
            item.innerHTML = `
                <span class="clarification-icon">❓</span>
                <span>${clarification}</span>
            `;
            content.appendChild(item);
        });
        
        card.style.display = 'block';
    }

    updateConfidence(elementPrefix, confidence) {
        const percentText = Math.round(confidence * 100) + '%';
        const confidenceText = document.getElementById(elementPrefix + '-text');
        const confidenceBar = document.getElementById(elementPrefix + '-bar');
        
        if (confidenceText) confidenceText.textContent = percentText;
        if (confidenceBar) {
            confidenceBar.style.width = percentText;
            
            // Update color based on confidence level
            confidenceBar.className = 'confidence-fill ';
            if (confidence >= 0.8) {
                confidenceBar.className += 'confidence-high';
            } else if (confidence >= 0.6) {
                confidenceBar.className += 'confidence-medium';
            } else {
                confidenceBar.className += 'confidence-low';
            }
        }
    }

    determineComplexity(generation) {
        const tableCount = generation.tables_used.length;
        const hasJoins = generation.sql_query.toLowerCase().includes('join');
        const hasAggregations = /count|sum|avg|max|min/i.test(generation.sql_query);
        
        if (tableCount > 3 || hasAggregations) return 'Complex';
        if (tableCount > 1 || hasJoins) return 'Moderate';
        return 'Simple';
    }

    showLoading() {
        document.getElementById('loading-section').classList.add('active');
        this.currentStep = 0;
        this.animateLoadingSteps();
    }

    hideLoading() {
        document.getElementById('loading-section').classList.remove('active');
        this.currentStep = 0;
        // Reset all loading steps
        this.loadingSteps.forEach(stepId => {
            const step = document.getElementById(stepId);
            if (step) {
                step.classList.remove('active', 'completed');
                step.querySelector('.loading-step-icon').textContent = '⏳';
            }
        });
    }

    animateLoadingSteps() {
        if (this.currentStep < this.loadingSteps.length) {
            // Mark previous step as completed
            if (this.currentStep > 0) {
                const prevStep = document.getElementById(this.loadingSteps[this.currentStep - 1]);
                if (prevStep) {
                    prevStep.classList.remove('active');
                    prevStep.classList.add('completed');
                    prevStep.querySelector('.loading-step-icon').textContent = '✅';
                }
            }
            
            // Mark current step as active
            const currentStep = document.getElementById(this.loadingSteps[this.currentStep]);
            if (currentStep) {
                currentStep.classList.add('active');
            }
            
            this.currentStep++;
            
            // Continue animation
            setTimeout(() => this.animateLoadingSteps(), 800);
        }
    }

    showSection(sectionId) {
        console.log(`Showing section: ${sectionId}`);
        const section = document.getElementById(sectionId);
        if (section) {
            console.log(`Section found: ${sectionId}, current classes:`, section.className);
            section.classList.remove('hidden');
            section.classList.add('visible');
            section.style.display = 'block'; // Force display
            console.log(`Section updated: ${sectionId}, new classes:`, section.className);
            
            // Scroll into view smoothly
            setTimeout(() => {
                section.scrollIntoView({ 
                    behavior: 'smooth', 
                    block: 'start' 
                });
            }, 500);
        } else {
            console.error(`Section not found: ${sectionId}`);
        }
    }

    hideSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            section.classList.remove('visible');
            section.classList.add('hidden');
        }
    }

    showExamples() {
        document.getElementById('examples-overlay').classList.add('active');
    }

    hideExamples() {
        document.getElementById('examples-overlay').classList.remove('active');
    }

    clearAll() {
        document.getElementById('question-input').value = '';
        this.hideSection('intent-section');
        this.hideSection('sql-section');
        this.hideSection('translation-section');
        this.intentAnalysis = null;
        this.sqlGeneration = null;
        
        // Focus back on input
        document.getElementById('question-input').focus();
    }

    refineQuestion() {
        // Scroll back to question input and focus
        document.getElementById('question-input').focus();
        document.getElementById('question-section').scrollIntoView({ 
            behavior: 'smooth', 
            block: 'start' 
        });
    }

    askNewQuestion() {
        this.clearAll();
    }

    async copySQLToClipboard() {
        if (!this.sqlGeneration) {
            this.showError('No SQL to copy.');
            return;
        }

        try {
            await navigator.clipboard.writeText(this.sqlGeneration.sql_query);
            this.showSuccess('SQL copied to clipboard!');
        } catch (error) {
            console.error('Failed to copy SQL:', error);
            this.showError('Failed to copy SQL to clipboard.');
        }
    }

    editSQL() {
        // For now, just copy to clipboard and suggest manual editing
        this.copySQLToClipboard();
        this.showInfo('SQL copied to clipboard. You can edit it manually and paste it into your database tool.');
    }

    autoResize(event) {
        const textarea = event.target;
        textarea.style.height = 'auto';
        textarea.style.height = Math.min(textarea.scrollHeight, 300) + 'px';
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    showSuccess(message) {
        this.showNotification(message, 'success');
    }

    showInfo(message) {
        this.showNotification(message, 'info');
    }

    showNotification(message, type = 'info') {
        // Simple notification system
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.textContent = message;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 8px;
            color: white;
            font-weight: 500;
            z-index: 10000;
            opacity: 0;
            transform: translateX(100%);
            transition: all 0.3s ease;
            max-width: 400px;
        `;

        // Set background color based on type
        const colors = {
            success: '#10B981',
            error: '#EF4444',
            warning: '#F59E0B',
            info: '#3B82F6'
        };
        notification.style.backgroundColor = colors[type] || colors.info;

        document.body.appendChild(notification);

        // Animate in
        setTimeout(() => {
            notification.style.opacity = '1';
            notification.style.transform = 'translateX(0)';
        }, 100);

        // Remove after delay
        setTimeout(() => {
            notification.style.opacity = '0';
            notification.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        }, 3000);
    }
}

// Global variable to hold the interface instance
let nlSqlInterface = null;

// Global function for onclick handler
function handleAnalyzeClick() {
    console.log('Global handleAnalyzeClick called');
    if (nlSqlInterface) {
        nlSqlInterface.analyzeQuestion();
    } else {
        console.error('NL to SQL interface not initialized');
        alert('Interface not ready. Please refresh the page.');
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing NL to SQL interface...');
    try {
        nlSqlInterface = new NLToSQLInterface();
        console.log('NL to SQL interface initialized successfully');
        
        // Make it globally accessible for debugging
        window.nlSqlInterface = nlSqlInterface;
    } catch (error) {
        console.error('Error initializing NL to SQL interface:', error);
    }
});

// Add some CSS for animations that we couldn't put in the template
const additionalCSS = `
    .animate-in {
        animation: slideInUp 0.6s ease-out forwards;
    }

    @keyframes slideInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    .nl-section.visible {
        animation: slideInUp 0.6s ease-out;
    }

    .confidence-fill {
        animation: fillBar 1s ease-out forwards;
    }

    @keyframes fillBar {
        from { width: 0; }
    }

    .example-item {
        position: relative;
        overflow: hidden;
    }

    .example-item:before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
        transition: left 0.5s;
    }

    .example-item:hover:before {
        left: 100%;
    }
`;

// Add the additional CSS to the page
const style = document.createElement('style');
style.textContent = additionalCSS;
document.head.appendChild(style);
