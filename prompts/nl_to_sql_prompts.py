"""
Natural Language to SQL Prompts

This module contains prompt templates for natural language to SQL translation.
"""

# Intent analysis prompt template
INTENT_ANALYSIS_PROMPT = """
You are an expert at understanding natural language questions about contract data and extracting structured intent.

## Available Canonical Schema
{canonical_schema}

## User's Question
"{user_question}"

## Your Task
Analyze this natural language question and extract the user's intent in structured format.

Consider:
1. What is the user trying to find out? (list, count, filter, aggregate, compare)
2. Which entity are they asking about? (contracts, parties, awards, etc.)
3. What fields do they want to see in the results?
4. What filter conditions are implied?
5. Are there any date ranges mentioned?
6. Do they want aggregations (count, sum, average)?
7. How should results be sorted?

## Date Understanding
- "Q1 2025" = January 1, 2025 to March 31, 2025
- "Q2 2025" = April 1, 2025 to June 30, 2025
- "Q3 2025" = July 1, 2025 to September 30, 2025
- "Q4 2025" = October 1, 2025 to December 31, 2025
- "next month" = calculate from current date
- "this year" = current calendar year
- "last year" = previous calendar year
- "expiring soon" = within next 30-90 days
- "this quarter" = current calendar quarter
- "last quarter" = previous calendar quarter

## Field Mapping Guide
- "value", "amount", "worth", "cost", "price" → value_amount
- "status", "state", "condition", "phase" → status
- "expiry", "expiration", "end date", "termination" → period_end
- "start", "begin", "commencement" → period_start
- "supplier", "vendor", "contractor", "provider" → supplier info
- "buyer", "client", "agency", "customer" → buyer info
- "contract ID", "identifier", "number" → contract_id
- "signed date", "signature date" → date_signed

## Status Value Mapping
- "active", "current", "ongoing", "live" → status = 'active'
- "expired", "terminated", "ended", "completed", "finished" → status = 'expired' or 'terminated'
- "pending", "in progress", "awaiting" → status = 'pending'
- "cancelled", "canceled", "void" → status = 'cancelled'

## Comparison Operators
- "more than", "greater than", "over", "above" → >
- "less than", "under", "below" → <
- "at least", "minimum" → >=
- "at most", "maximum" → <=
- "equals", "is", "exactly" → =
- "between" → BETWEEN
- "contains", "includes", "like" → LIKE

Respond with JSON following this exact structure:
{{
    "query_intent": "list_contracts|filter_contracts|count_contracts|aggregate_values|compare_periods|find_expiring|unknown",
    "primary_entity": "contracts",
    "requested_fields": ["contract_id", "value_amount", "status"],
    "filter_conditions": [
        {{
            "field": "status",
            "operator": "=",
            "value": "active",
            "confidence": 0.9,
            "original_text": "active contracts"
        }}
    ],
    "date_ranges": [
        {{
            "start_date": "2025-01-01",
            "end_date": "2025-03-31",
            "confidence": 0.9,
            "original_text": "Q1 2025"
        }}
    ],
    "aggregations": [],
    "sort_fields": [["value_amount", "DESC"]],
    "confidence": 0.85,
    "assumptions": ["'active' means status='active'", "Q1 means calendar quarter"],
    "clarifications_needed": [],
    "original_query": "{user_question}"
}}
"""

# SQL generation prompt template
SQL_GENERATION_PROMPT = """
You are a SQL expert who writes DuckDB queries against a canonical contract schema.

## TARGET DATABASE: DuckDB
Generate SQL that is 100% compatible with DuckDB syntax.

## Canonical Schema
{canonical_schema}

## User Intent Analysis
{intent_context}

## Your Task
Generate a DuckDB SQL query that fulfills the user's intent using ONLY the canonical schema fields above.

## Schema Rules
The canonical schema has these main tables:
- contracts: Main contract information
- parties: Organizations involved (buyers, suppliers)
- awards: Award decisions
- items: Contract line items
- documents: Contract documents
- implementation_transactions: Payment transactions

## CRITICAL: DuckDB-Specific Syntax Rules
1. **Arrays/Lists**: Use DuckDB list syntax [...] NOT ARRAY[...]
   - CORRECT: [value1, value2, value3]
   - WRONG: ARRAY[value1, value2, value3]

2. **List Operations**: Use DuckDB list functions
   - To check if value in list: value = ANY(list_column)
   - To filter nulls: list_filter(list_column, x -> x IS NOT NULL)
   - DO NOT use ARRAY_REMOVE or other PostgreSQL array functions

3. **Date Functions**: Use DuckDB date functions
   - Current date: CURRENT_DATE
   - Date arithmetic: date_column + INTERVAL '30 days'
   - Date comparison: period_end >= CAST('2025-01-01' AS DATE)

4. **String Functions**: Use DuckDB string functions
   - Pattern matching: column LIKE '%pattern%'
   - Case insensitive: LOWER(column) = 'value'
   - String concatenation: column || ' text'

## Required SQL Guidelines
1. Use ONLY table and column names from the canonical schema above
2. Handle date ranges properly with BETWEEN or >= <= operators
3. Map status conditions appropriately using enum values
4. Include proper JOINs if multiple tables are needed
5. Add appropriate sorting (ORDER BY) unless specifically requested otherwise
6. Use proper aggregation functions (COUNT, SUM, AVG, MAX, MIN) if needed
7. **IMPORTANT**: Ensure the query uses DuckDB syntax, NOT PostgreSQL
8. Use table aliases for readability (c for contracts, p for parties, etc.)
9. **Avoid complex array operations** - keep queries simple when possible

## Field Mapping Rules
- For contract value: Use contracts.value_amount
- For contract status: Use contracts.status (enum values: pending, active, cancelled, terminated, expired)
- For contract dates: Use contracts.period_start, contracts.period_end, contracts.date_signed
- For parties: JOIN with parties table using contracts.supplier_party_ids or contracts.buyer_party_id
- For contract ID: Use contracts.contract_id
- For currency: Use contracts.value_currency

## Status Values (Exact Enum Values)
- "active" → status = 'active'
- "expired" → status = 'expired'
- "terminated" → status = 'terminated'
- "pending" → status = 'pending'
- "cancelled" → status = 'cancelled'

## DuckDB JOIN Examples (Correct Syntax)
- Simple join: JOIN parties p ON p.party_id = c.buyer_party_id
- For awards: JOIN awards a ON a.award_id = c.award_id
- **Avoid complex list joins** - use simpler approaches when possible

## Common Query Patterns (DuckDB Compatible)
1. Simple list: SELECT contract_id, status, value_amount FROM contracts WHERE ...
2. With party info: SELECT c.contract_id, p.name FROM contracts c JOIN parties p ON p.party_id = c.buyer_party_id
3. Aggregation: SELECT COUNT(*) as count, SUM(value_amount) as total FROM contracts WHERE ...
4. Date filtering: WHERE period_end BETWEEN CAST('2025-01-01' AS DATE) AND CAST('2025-03-31' AS DATE)

## IMPORTANT: Keep It Simple
- Prefer straightforward queries over complex ones
- Avoid unnecessary array/list operations
- Use basic JOINs rather than complex subqueries when possible
- Focus on getting the data the user needs with minimal complexity

## Output Requirements
Generate ONLY the SQL query, no explanations or comments.
The query must be executable in DuckDB database.
Use proper formatting with line breaks for readability.
VERIFY your query uses DuckDB syntax, NOT PostgreSQL syntax.
"""

# Examples for the UI
EXAMPLE_QUERIES = {
    "getting_started": [
        "Show me all active contracts",
        "Which contracts expire next month?",
        "List all contracts from 2024",
        "How many contracts do we have?",
        "Show me contract details"
    ],
    "value_based": [
        "Show contracts worth more than $500,000",
        "What are our highest value contracts?",
        "Find contracts between $100K and $1M",
        "Which contracts are under $50,000?",
        "Show me the most expensive contracts"
    ],
    "time_based": [
        "Contracts expiring in the next 30 days",
        "Show contracts created this quarter",
        "Which contracts expired last year?",
        "Find contracts signed in Q1 2024",
        "Show contracts ending this month"
    ],
    "party_based": [
        "Show all contracts with Acme Corporation",
        "Which suppliers have active contracts?",
        "Find contracts for government agencies",
        "List all contracts with Microsoft",
        "Show me our top suppliers by contract value"
    ],
    "complex_queries": [
        "Show me the total value of all active contracts",
        "Which contracts were amended in the last 6 months?",
        "Compare Q1 vs Q2 contract values",
        "Average contract value by supplier",
        "Count contracts by status"
    ],
    "status_queries": [
        "Show all terminated contracts",
        "Find expired contracts from last year",
        "Which contracts are pending approval?",
        "List cancelled contracts",
        "Show active contracts expiring soon"
    ]
}

# Clarification prompts for ambiguous queries
CLARIFICATION_PROMPTS = {
    "ambiguous_value": "When you mentioned 'value', did you mean:\n• Total contract value\n• Annual recurring revenue\n• Monthly payment amount",
    "ambiguous_date": "For the date range '{original_text}', did you mean:\n• Calendar year/quarter\n• Fiscal year/quarter\n• Exact date range",
    "ambiguous_status": "When you said '{status_term}', did you mean:\n• Currently active contracts\n• All contracts regardless of status\n• Specific status like 'pending' or 'terminated'",
    "ambiguous_party": "When you mentioned '{party_name}', did you mean:\n• Exact company name match\n• Companies containing that name\n• All subsidiaries included",
    "missing_sort": "How would you like the results sorted?\n• By contract value (highest first)\n• By expiration date (soonest first)\n• By contract start date\n• Alphabetically by supplier name"
}

# Validation messages
VALIDATION_MESSAGES = {
    "no_results_expected": "This query might not return any results because {reason}. Would you like to modify it?",
    "large_result_set": "This query might return a very large number of results. Consider adding filters to narrow it down.",
    "complex_query": "This is a complex query involving {table_count} tables. Results may take longer to process.",
    "date_range_warning": "The date range spans {days} days, which might return many results.",
    "ambiguous_field": "The field '{field}' could refer to multiple things. I assumed you meant '{assumed_field}'."
}

# Response templates for the UI
UI_RESPONSE_TEMPLATES = {
    "intent_understood": "I understand you want to {intent_description}.",
    "assumptions_made": "I'm making these assumptions: {assumptions}",
    "clarification_needed": "I need clarification on: {clarifications}",
    "confidence_high": "I'm confident I understood your question correctly.",
    "confidence_medium": "I think I understood your question, but please verify the details below.",
    "confidence_low": "I'm not entirely sure I understood correctly. Please review my interpretation."
}

def build_intent_analysis_prompt(user_question: str, canonical_schema: str) -> str:
    """Build the intent analysis prompt with user question and schema context"""
    return INTENT_ANALYSIS_PROMPT.format(
        canonical_schema=canonical_schema,
        user_question=user_question
    )

def build_sql_generation_prompt(canonical_schema: str, intent_context: str) -> str:
    """Build the SQL generation prompt with schema and intent context"""
    return SQL_GENERATION_PROMPT.format(
        canonical_schema=canonical_schema,
        intent_context=intent_context
    )

def get_example_queries_by_category() -> dict:
    """Get example queries organized by category"""
    return EXAMPLE_QUERIES

def get_clarification_prompt(clarification_type: str, **kwargs) -> str:
    """Get clarification prompt for specific ambiguity types"""
    template = CLARIFICATION_PROMPTS.get(clarification_type, "Please clarify your question.")
    return template.format(**kwargs)

def get_validation_message(validation_type: str, **kwargs) -> str:
    """Get validation message for specific scenarios"""
    template = VALIDATION_MESSAGES.get(validation_type, "Please review the query.")
    return template.format(**kwargs)
