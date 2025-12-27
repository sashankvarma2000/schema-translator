"""OpenAI LLM adapter for schema mapping."""

import json
import logging
from pathlib import Path
from typing import Optional

import openai
from openai import OpenAI

from ..core.config import settings

# Get module logger
module_logger = logging.getLogger(__name__)
from ..shared.logging import logger
from ..shared.models import LLMResponse

# JSON Schemas for Structured Outputs
JOIN_STRATEGY_SCHEMA = {
    "type": "object",
    "properties": {
        "primary_table": {
            "type": "string",
            "description": "The main table to start the query from"
        },
        "join_tables": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 3,
                "maxItems": 3
            },
            "description": "List of JOIN operations as [table, condition, join_type]"
        },
        "join_order": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Ordered list of tables in the JOIN sequence"
        },
        "confidence": {
            "type": "number",
            "minimum": 0,
            "maximum": 1,
            "description": "Confidence score for the JOIN strategy"
        },
        "reasoning": {
            "type": "string",
            "description": "Explanation of the JOIN strategy decision"
        },
        "performance_notes": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Performance optimization notes"
        }
    },
    "required": ["primary_table", "join_tables", "join_order", "confidence", "reasoning", "performance_notes"],
    "additionalProperties": False
}

RELATIONSHIP_SCHEMA = {
    "type": "object",
    "properties": {
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table1": {"type": "string", "description": "First table in the relationship"},
                    "column1": {"type": "string", "description": "Column from first table"},
                    "table2": {"type": "string", "description": "Second table in the relationship"},
                    "column2": {"type": "string", "description": "Column from second table"},
                    "relationship_type": {
                        "type": "string",
                        "enum": ["one_to_one", "one_to_many", "many_to_one", "many_to_many", "logical_entity"],
                        "description": "Type of relationship between tables"
                    },
                    "join_condition": {
                        "type": "string",
                        "description": "SQL JOIN condition (e.g., 'table1.col1 = table2.col2')"
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Confidence score for this relationship (0-1)"
                    },
                    "reasoning": {"type": "string", "description": "Explanation of why this relationship exists"},
                    "is_primary_key": {"type": "boolean", "description": "Whether column1 is a primary key", "default": False},
                    "is_foreign_key": {"type": "boolean", "description": "Whether column1 is a foreign key", "default": False}
                },
                "required": ["table1", "column1", "table2", "column2", "relationship_type", "join_condition", "confidence", "reasoning", "is_primary_key", "is_foreign_key"],
                "additionalProperties": False
            }
        }
    },
    "required": ["relationships"],
    "additionalProperties": False
}


class OpenAIAdapter:
    """OpenAI API adapter for LLM-powered schema mapping."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.openai_api_key
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        # Add timeout to prevent hanging
        # Note: GPT-5 responses API can be slower, so we use a longer timeout
        # but reduce retries to fail faster
        self.client = OpenAI(
            api_key=self.api_key,
            timeout=120.0,  # 120 second timeout for GPT-5 responses API
            max_retries=1  # Reduce retries to fail faster on errors
        )
        self.model = settings.openai_model
        self.temperature = settings.openai_temperature
        self.max_tokens = settings.openai_max_tokens
        
        # Load prompt template
        self.prompt_template = self._load_prompt_template()
        
    def _load_prompt_template(self) -> str:
        """Load the column mapping prompt template."""
        prompt_path = settings.prompts_dir / "column_mapping_v1.txt"
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt template not found: {prompt_path}")
        
        with open(prompt_path, 'r') as f:
            return f.read()
    
    def map_column(
        self,
        canonical_schema_excerpt: str,
        tenant: str,
        table: str,
        column: str,
        column_samples: list[str],
        cooccurring_columns: list[str],
        column_type: str,
        description: Optional[str] = None,
        additional_context: Optional[str] = None
    ) -> LLMResponse:
        """
        Get LLM mapping proposal for a source column.
        
        Args:
            canonical_schema_excerpt: Relevant fields from canonical schema
            tenant: Tenant identifier
            table: Source table name
            column: Source column name
            column_samples: Sample values from the column
            cooccurring_columns: Other columns in the same table
            column_type: Inferred column type
            description: Optional column description
            additional_context: Any additional context
            
        Returns:
            LLMResponse with mapping proposals
        """
        
        # Build the user prompt with source context
        user_prompt = self._build_user_prompt(
            canonical_schema_excerpt=canonical_schema_excerpt,
            tenant=tenant,
            table=table,
            column=column,
            column_samples=column_samples,
            cooccurring_columns=cooccurring_columns,
            column_type=column_type,
            description=description,
            additional_context=additional_context
        )
        
        try:
            # Use the new responses API for GPT-5 models
            if "gpt-5" in self.model:
                # Combine system and user prompts for GPT-5
                combined_prompt = f"{self.prompt_template}\n\n{user_prompt}"
                response = self.client.responses.create(
                    model=self.model,
                    input=combined_prompt
                )
                
                # Extract text from the response structure
                if response.output and len(response.output) > 0:
                    content = response.output[0].get('content', [])
                    if content and len(content) > 0:
                        response_text = content[0].get('text', '')
                    else:
                        response_text = ""
                else:
                    response_text = ""
            else:
                # Fallback to chat completions for other models
                completion_params = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": self.prompt_template},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": self.temperature,
                    "response_format": {"type": "json_object"}
                }
                
                # Use max_completion_tokens for newer models, max_tokens for older ones
                if "gpt-4o" in self.model or "o1" in self.model:
                    completion_params["max_completion_tokens"] = self.max_tokens
                else:
                    completion_params["max_tokens"] = self.max_tokens
                
                response = self.client.chat.completions.create(**completion_params)
                response_text = response.choices[0].message.content
            
            module_logger.debug(f"LLM response for {tenant}.{table}.{column}: {response_text}")
            
            # Parse JSON response
            response_data = json.loads(response_text)
            return LLMResponse(**response_data)
            
        except json.JSONDecodeError as e:
            module_logger.error(f"Failed to parse LLM response as JSON: {e}")
            module_logger.error(f"Raw response: {response_text}")
            raise ValueError(f"Invalid JSON response from LLM: {e}")
            
        except openai.OpenAIError as e:
            module_logger.error(f"OpenAI API error: {e}")
            raise RuntimeError(f"LLM request failed: {e}")
            
        except Exception as e:
            module_logger.error(f"Unexpected error in LLM mapping: {e}")
            raise
    
    def _build_user_prompt(
        self,
        canonical_schema_excerpt: str,
        tenant: str,
        table: str,
        column: str,
        column_samples: list[str],
        cooccurring_columns: list[str],
        column_type: str,
        description: Optional[str] = None,
        additional_context: Optional[str] = None
    ) -> str:
        """Build the user prompt with source column context."""
        
        prompt_parts = [
            "## Canonical Schema Fields",
            canonical_schema_excerpt,
            "",
            "## Source Column Context",
            f"- **Tenant**: {tenant}",
            f"- **Table**: {table}",
            f"- **Column**: {column}",
            f"- **Inferred Type**: {column_type}",
        ]
        
        if description:
            prompt_parts.append(f"- **Description**: {description}")
        
        # Add sample values
        if column_samples:
            sample_str = ", ".join(f'"{sample}"' for sample in column_samples[:10])
            prompt_parts.extend([
                f"- **Sample Values**: [{sample_str}]"
            ])
        
        # Add co-occurring columns
        if cooccurring_columns:
            cooccur_str = ", ".join(cooccurring_columns[:5])
            prompt_parts.append(f"- **Co-occurring Columns**: [{cooccur_str}]")
        
        # Add additional context
        if additional_context:
            prompt_parts.extend([
                "",
                "## Additional Context",
                additional_context
            ])
        
        prompt_parts.extend([
            "",
            "## Your Task",
            f"Analyze the source column '{column}' and propose mapping(s) to the canonical schema.",
            "Remember to be conservative with confidence scores and provide clear justifications.",
            "",
            "Respond with valid JSON following the specified schema:"
        ])
        
        return "\n".join(prompt_parts)
    
    def generate_completion(self, prompt: str, json_schema: dict = None) -> str:
        """Generate completion for a given prompt with optional structured outputs."""
        import time
        start_time = time.time()
        try:
            # Use the new responses API for GPT-5 models
            if "gpt-5" in self.model:
                module_logger.info(f"🤖 Using GPT-5 responses API (model: {self.model})")
                module_logger.debug(f"Prompt length: {len(prompt)} characters")
                request_params = {
                    "model": self.model,
                    "input": prompt
                }
                
                # Add structured outputs if schema is provided
                if json_schema:
                    request_params["text"] = {
                        "format": {
                            "type": "json_schema",
                            "name": "structured_response",
                            "strict": True,
                            "schema": json_schema
                        }
                    }
                
                response = self.client.responses.create(**request_params)
                elapsed = time.time() - start_time
                module_logger.info(f"✅ GPT-5 response received in {elapsed:.2f}s")
                module_logger.debug(f"GPT-5 response type: {type(response)}")
                module_logger.debug(f"GPT-5 response attributes: {dir(response)}")
                
                # Extract text from the response structure
                # Try multiple possible response formats
                response_text = None
                
                # Method 1: Direct output_text attribute
                if hasattr(response, 'output_text') and response.output_text:
                    response_text = response.output_text
                    module_logger.debug("Extracted text from response.output_text")
                
                # Method 2: output array with content
                elif hasattr(response, 'output') and response.output:
                    if isinstance(response.output, list) and len(response.output) > 0:
                        first_output = response.output[0]
                        # Check if it's a dict with 'content'
                        if isinstance(first_output, dict) and 'content' in first_output:
                            content = first_output['content']
                            if isinstance(content, list) and len(content) > 0:
                                first_content = content[0]
                                if isinstance(first_content, dict) and 'text' in first_content:
                                    response_text = first_content['text']
                                    module_logger.debug("Extracted text from response.output[0].content[0].text")
                        # Check if it's an object with content attribute
                        elif hasattr(first_output, 'content'):
                            content = first_output.content
                            if hasattr(content, '__iter__') and len(list(content)) > 0:
                                first_content = list(content)[0]
                                if hasattr(first_content, 'text'):
                                    response_text = first_content.text
                                    module_logger.debug("Extracted text from response.output[0].content[0].text (object)")
                                elif isinstance(first_content, dict) and 'text' in first_content:
                                    response_text = first_content['text']
                                    module_logger.debug("Extracted text from response.output[0].content[0].text (dict)")
                
                # Method 3: Try accessing as dict
                elif isinstance(response, dict):
                    if 'output_text' in response:
                        response_text = response['output_text']
                    elif 'output' in response and isinstance(response['output'], list) and len(response['output']) > 0:
                        output = response['output'][0]
                        if isinstance(output, dict):
                            if 'text' in output:
                                response_text = output['text']
                            elif 'content' in output and isinstance(output['content'], list) and len(output['content']) > 0:
                                content = output['content'][0]
                                if isinstance(content, dict) and 'text' in content:
                                    response_text = content['text']
                
                if response_text:
                    module_logger.debug(f"Successfully extracted response text ({len(response_text)} characters)")
                    return response_text
                else:
                    module_logger.warning(f"No text content found in GPT-5 response. Response structure: {response}")
                    # Log the full response for debugging
                    module_logger.debug(f"Full response: {response}")
                    return ""
            else:
                # Fallback to chat completions for other models
                completion_params = {
                    "model": self.model,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": self.temperature
                }
                
                # Add structured outputs for supported models if schema provided
                if json_schema and self.model in ['gpt-4o-mini', 'gpt-4o-2024-08-06', 'gpt-4o']:
                    completion_params["response_format"] = {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "structured_response",
                            "strict": True,
                            "schema": json_schema
                        }
                    }
                else:
                    # Use basic JSON mode as fallback
                    completion_params["response_format"] = {"type": "json_object"}
                
                # Use max_completion_tokens for newer models, max_tokens for older ones
                if "gpt-4o" in self.model or "o1" in self.model:
                    completion_params["max_completion_tokens"] = self.max_tokens
                else:
                    completion_params["max_tokens"] = self.max_tokens
                
                response = self.client.chat.completions.create(**completion_params)
                elapsed = time.time() - start_time
                module_logger.info(f"✅ Chat completion received in {elapsed:.2f}s")
                return response.choices[0].message.content
            
        except Exception as e:
            elapsed = time.time() - start_time
            module_logger.error(f"❌ OpenAI API error after {elapsed:.2f}s: {e}")
            module_logger.error(f"Error type: {type(e).__name__}")
            raise
    
    def generate_completion_raw(self, prompt: str):
        """Generate completion and return raw response object for debugging."""
        try:
            if "gpt-5" in self.model:
                return self.client.responses.create(
                    model=self.model,
                    input=prompt
                )
            else:
                completion_params = {
                    "model": self.model,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": self.temperature,
                    "response_format": {"type": "json_object"}
                }
                
                if "gpt-4o" in self.model or "o1" in self.model:
                    completion_params["max_completion_tokens"] = self.max_tokens
                else:
                    completion_params["max_tokens"] = self.max_tokens
                
                return self.client.chat.completions.create(**completion_params)
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            raise
    
    def parse_json_response(self, response) -> dict:
        """Parse JSON response from LLM"""
        try:
            import json
            import re
            module_logger.debug(f"Parsing response of type: {type(response)}")
            
            # Extract text content first
            text_content = None
            if isinstance(response, str):
                text_content = response
            elif hasattr(response, 'output_text'):
                # Direct output_text property (GPT-5)
                text_content = response.output_text
            elif hasattr(response, 'output') and response.output:
                # Handle GPT-5 response structure: response.output[0].content[0].text
                if len(response.output) > 0:
                    first_output = response.output[0]
                    if hasattr(first_output, 'content') and first_output.content and len(first_output.content) > 0:
                        first_content = first_output.content[0]
                        if hasattr(first_content, 'text'):
                            text_content = first_content.text
                        elif isinstance(first_content, dict) and 'text' in first_content:
                            text_content = first_content['text']
            elif hasattr(response, 'get'):
                # Handle dictionary-like response
                return response
            else:
                # Try to convert to string
                text_content = str(response)
            
            if text_content:
                    # Clean up the JSON string to handle control characters and formatting issues
                    # Remove or replace problematic control characters including zero-width spaces
                    cleaned_text = re.sub(r'[\x00-\x1f\x7f-\x9f\u200b-\u200d\ufeff]', '', text_content)
                    
                    # Remove any BOM or invisible Unicode characters
                    cleaned_text = cleaned_text.encode('utf-8').decode('utf-8-sig')
                    
                    # Fix common JSON formatting issues
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)  # Normalize whitespace
                    cleaned_text = cleaned_text.strip()
                    
                    # Fix common JSON formatting issues that cause parsing errors
                    # Fix line breaks within strings
                    cleaned_text = re.sub(r'"\s*\n\s*', '" ', cleaned_text)
                    # Fix trailing commas
                    cleaned_text = re.sub(r',\s*([}\]])', r'\1', cleaned_text)
                    # Fix missing commas between array elements
                    cleaned_text = re.sub(r'"\s*\]\s*\[\s*"', '"], ["', cleaned_text)
                    
                    # Extra cleaning for problematic characters at specific positions
                    # Remove any non-printable characters right after opening braces/brackets
                    cleaned_text = re.sub(r'([{[])\s*[^\w"]+', r'\1', cleaned_text)
                
                    # Try to extract and parse JSON from the cleaned text
                    # First try to parse as-is (might be a complete JSON object or array)
                    try:
                        parsed = json.loads(cleaned_text)
                        # If it's an array, return it directly (for relationship discovery)
                        if isinstance(parsed, list):
                            return parsed
                        # If it's an object, return it
                        elif isinstance(parsed, dict):
                            return parsed
                        else:
                            return parsed
                    except json.JSONDecodeError as e:
                        module_logger.error(f"JSON parse failed: {e}")
                        module_logger.error(f"Problematic JSON text (first 500 chars): {repr(cleaned_text[:500])}")
                        module_logger.error(f"Character codes at error position: {[ord(c) for c in cleaned_text[:10]]}")
                        
                        # Try to fix the JSON first
                        fixed_json = self._fix_malformed_json(cleaned_text)
                        module_logger.debug(f"Fixed JSON (first 200 chars): {repr(fixed_json[:200])}")
                        try:
                            return json.loads(fixed_json)
                        except json.JSONDecodeError as e2:
                            module_logger.error(f"Fixed JSON also failed: {e2}")
                            module_logger.debug(f"Fixed JSON also failed, trying extraction...")
                            
                            # If that fails, try to extract JSON object boundaries
                            json_match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(0)
                                try:
                                    return json.loads(json_str)
                                except json.JSONDecodeError:
                                    # Try to fix the extracted JSON
                                    fixed_extracted = self._fix_malformed_json(json_str)
                                    return json.loads(fixed_extracted)
                            else:
                                # Try to extract JSON array boundaries
                                array_match = re.search(r'\[.*\]', cleaned_text, re.DOTALL)
                                if array_match:
                                    json_str = array_match.group(0)
                                    try:
                                        return json.loads(json_str)
                                    except json.JSONDecodeError:
                                        # Try to fix the extracted JSON
                                        fixed_extracted = self._fix_malformed_json(json_str)
                                        return json.loads(fixed_extracted)
                                else:
                                    raise json.JSONDecodeError("No valid JSON found", cleaned_text, 0)
            
            # Fallback
            return []
            
        except (json.JSONDecodeError, AttributeError) as e:
            # Use the module logger since self.logger doesn't exist
            module_logger.error(f"Failed to parse JSON response: {e}")
            module_logger.error(f"Response type: {type(response)}")
            module_logger.error(f"Response content preview: {str(response)[:200] if response else 'None'}...")
            # Return empty list as fallback for relationship discovery
            return []
    
    def _fix_malformed_json(self, json_str: str) -> str:
        """Attempt to fix common JSON formatting issues"""
        try:
            import re
            
            # Fix common issues
            fixed = json_str.strip()
            
            # Remove any leading/trailing whitespace and control characters
            fixed = re.sub(r'^[\s\x00-\x1f\x7f-\x9f]*', '', fixed)
            fixed = re.sub(r'[\s\x00-\x1f\x7f-\x9f]*$', '', fixed)
            
            # Fix unescaped newlines in strings (but preserve JSON structure newlines)
            fixed = re.sub(r'(?<!\\)\n(?![}\],])', '\\n', fixed)
            
            # Fix trailing commas before closing braces/brackets
            fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
            
            # Fix missing commas between object properties (be more careful)
            fixed = re.sub(r'"\s*\n\s*"(?=\w)', '",\n"', fixed)
            
            # Fix missing quotes around unquoted property names (but be careful not to break already quoted ones)
            fixed = re.sub(r'(\n\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', fixed)
            
            # Normalize whitespace but preserve structure
            fixed = re.sub(r'\s+', ' ', fixed)
            fixed = re.sub(r'\s*([{}\[\],:])\s*', r'\1', fixed)
            fixed = re.sub(r'([{}\[\],:])', r'\1 ', fixed)
            fixed = re.sub(r'\s+', ' ', fixed)
            fixed = fixed.strip()
            
            return fixed
            
        except Exception as e:
            # Use the module logger since self.logger doesn't exist
            module_logger.warning(f"Failed to fix malformed JSON: {e}")
            return json_str


class MockLLMAdapter(OpenAIAdapter):
    """Mock LLM adapter for testing without API calls."""
    
    def __init__(self):
        # Skip OpenAI client initialization
        self.model = "mock-model"
        self.temperature = 0.1
        self.max_tokens = 2000
        
        # Load prompt template
        try:
            self.prompt_template = self._load_prompt_template()
        except FileNotFoundError:
            self.prompt_template = "Mock prompt template"
    
    def map_column(self, **kwargs) -> LLMResponse:
        """Enhanced mock response with better semantic understanding."""
        column = kwargs.get('column', 'unknown_column')
        description = kwargs.get('description', '')
        
        column_lower = column.lower()
        desc_lower = description.lower()
        
        # Enhanced pattern-based mock responses with better semantic understanding
        
        # ID and Identifier patterns
        if any(keyword in column_lower for keyword in ['id', 'identifier', 'key', 'number']):
            if 'award' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "award_id",
                        "justification": f"Column '{column}' appears to be an award identifier",
                        "confidence": 0.90,
                        "assumptions": ["Column represents unique award identifier"]
                    }],
                    reasoning="Mock response for award ID-like column"
                )
            elif 'contract' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "contract_id",
                        "justification": f"Column '{column}' appears to be a contract identifier",
                        "confidence": 0.90,
                        "assumptions": ["Column represents unique contract identifier"]
                    }],
                    reasoning="Mock response for contract ID-like column"
                )
            elif 'party' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "party_id",
                        "justification": f"Column '{column}' appears to be a party identifier",
                        "confidence": 0.90,
                        "assumptions": ["Column represents unique party identifier"]
                    }],
                    reasoning="Mock response for party ID-like column"
                )
            elif 'transaction' in column_lower or 'action' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "transaction_id",
                        "justification": f"Column '{column}' appears to be a transaction identifier",
                        "confidence": 0.90,
                        "assumptions": ["Column represents unique transaction identifier"]
                    }],
                    reasoning="Mock response for transaction ID-like column"
                )
            else:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "contract_id",
                        "justification": f"Column '{column}' appears to be an identifier",
                        "confidence": 0.85,
                        "assumptions": ["Column represents unique identifier"]
                    }],
                    reasoning="Mock response for ID-like column"
                )
        
        # Financial/Value patterns
        elif any(keyword in column_lower for keyword in ['amount', 'value', 'price', 'cost', 'obligation']):
            if 'total' in column_lower or 'current' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "total_value",
                        "justification": f"Column '{column}' appears to contain total value information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents total contract value"]
                    }],
                    reasoning="Mock response for total value column"
                )
            elif 'obligation' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "obligated_amount",
                        "justification": f"Column '{column}' appears to contain obligation amount",
                        "confidence": 0.85,
                        "assumptions": ["Column represents obligated amount"]
                    }],
                    reasoning="Mock response for obligation amount column"
                )
            else:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "value_amount",
                        "justification": f"Column '{column}' appears to contain financial information",
                        "confidence": 0.80,
                        "assumptions": ["Column represents financial amount"]
                    }],
                    reasoning="Mock response for financial column"
                )
        
        # Date patterns
        elif any(keyword in column_lower for keyword in ['date', 'time']):
            if 'award' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "award_date",
                        "justification": f"Column '{column}' appears to contain award date information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents award date"]
                    }],
                    reasoning="Mock response for award date column"
                )
            elif 'sign' in column_lower or 'signature' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "date_signed",
                        "justification": f"Column '{column}' appears to contain signature date information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents contract signature date"]
                    }],
                    reasoning="Mock response for signature date column"
                )
            elif 'start' in column_lower or 'begin' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "period_start",
                        "justification": f"Column '{column}' appears to contain period start date information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents period start date"]
                    }],
                    reasoning="Mock response for period start date column"
                )
            elif 'end' in column_lower or 'expir' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "period_end",
                        "justification": f"Column '{column}' appears to contain period end date information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents period end date"]
                    }],
                    reasoning="Mock response for period end date column"
                )
            else:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "date_signed",
                        "justification": f"Column '{column}' appears to contain date information",
                        "confidence": 0.75,
                        "assumptions": ["Column represents date information"]
                    }],
                    reasoning="Mock response for date-like column"
                )
        
        # Name patterns
        elif 'name' in column_lower:
            if 'supplier' in column_lower or 'vendor' in column_lower or 'recipient' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "supplier_name",
                        "justification": f"Column '{column}' appears to contain supplier name information",
                        "confidence": 0.90,
                        "assumptions": ["Column represents supplier name"]
                    }],
                    reasoning="Mock response for supplier name column"
                )
            elif 'buyer' in column_lower or 'agency' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "buyer_name",
                        "justification": f"Column '{column}' appears to contain buyer name information",
                        "confidence": 0.90,
                        "assumptions": ["Column represents buyer name"]
                    }],
                    reasoning="Mock response for buyer name column"
                )
            else:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "supplier_name",
                        "justification": f"Column '{column}' appears to contain name information",
                        "confidence": 0.80,
                        "assumptions": ["Column represents organization name"]
                    }],
                    reasoning="Mock response for name column"
                )
        
        # Status patterns
        elif any(keyword in column_lower for keyword in ['status', 'state', 'phase']):
            return LLMResponse(
                proposed_mappings=[{
                    "canonical_field": "status",
                    "justification": f"Column '{column}' appears to contain status information",
                    "confidence": 0.85,
                    "assumptions": ["Column represents status information"]
                }],
                reasoning="Mock response for status column"
            )
        
        # Agency patterns
        elif 'agency' in column_lower:
            if 'awarding' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "awarding_agency",
                        "justification": f"Column '{column}' appears to contain awarding agency information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents awarding agency"]
                    }],
                    reasoning="Mock response for awarding agency column"
                )
            elif 'funding' in column_lower:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "funding_agency",
                        "justification": f"Column '{column}' appears to contain funding agency information",
                        "confidence": 0.85,
                        "assumptions": ["Column represents funding agency"]
                    }],
                    reasoning="Mock response for funding agency column"
                )
            else:
                return LLMResponse(
                    proposed_mappings=[{
                        "canonical_field": "awarding_agency",
                        "justification": f"Column '{column}' appears to contain agency information",
                        "confidence": 0.80,
                        "assumptions": ["Column represents agency information"]
                    }],
                    reasoning="Mock response for agency column"
                )
        
        # Description/Title patterns
        elif any(keyword in column_lower for keyword in ['description', 'title', 'comment']):
            return LLMResponse(
                proposed_mappings=[{
                    "canonical_field": "description",
                    "justification": f"Column '{column}' appears to contain descriptive information",
                    "confidence": 0.80,
                    "assumptions": ["Column represents descriptive text"]
                }],
                reasoning="Mock response for description column"
            )
        
        # Type patterns
        elif 'type' in column_lower:
            return LLMResponse(
                proposed_mappings=[{
                    "canonical_field": "contract_type",
                    "justification": f"Column '{column}' appears to contain type information",
                    "confidence": 0.80,
                    "assumptions": ["Column represents type classification"]
                }],
                reasoning="Mock response for type column"
            )
        
        # Currency patterns
        elif 'currency' in column_lower:
            return LLMResponse(
                proposed_mappings=[{
                    "canonical_field": "currency",
                    "justification": f"Column '{column}' appears to contain currency information",
                    "confidence": 0.85,
                    "assumptions": ["Column represents currency code"]
                }],
                reasoning="Mock response for currency column"
            )
        
        # Default fallback
        else:
            return LLMResponse(
                proposed_mappings=[],
                alternatives=[{
                    "canonical_field": "contract_id",
                    "confidence": 0.3,
                    "note": "Mock low-confidence mapping"
                }],
                reasoning=f"Mock response for column '{column}' - no clear mapping"
            )
