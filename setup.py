#!/usr/bin/env python3
"""
Setup script for Schema Translator.

This script helps set up the development environment and run initial tests.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"   Command: {cmd}")
        print(f"   Error: {e.stderr}")
        return False


def main():
    """Set up the schema translator environment."""
    
    print("🚀 Schema Translator Setup")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Error: pyproject.toml not found. Please run this from the project root.")
        sys.exit(1)
    
    # Install dependencies
    if not run_command("pip install -e .", "Installing package in development mode"):
        print("\n❌ Setup failed. Please check the error messages above.")
        sys.exit(1)
    
    # Create output directories
    print("\n📁 Creating directories...")
    directories = [
        "customer_schemas",
        "customer_samples", 
        "prompts",
        "output",
        "tests"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"   Created: {directory}/")
    
    print("✅ Directory structure created")
    
    # Test basic imports
    print("\n🧪 Testing basic functionality...")
    test_script = """
import sys
sys.path.insert(0, 'src')

try:
    from app.core.discovery import SchemaDiscoverer
    from app.core.llm_mapper import LLMMapper
    from app.shared.models import ColumnType
    print("✅ Core imports successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

# Test with sample data if available
try:
    from pathlib import Path
    if (Path("customer_schemas") / "tenant_A").exists():
        discoverer = SchemaDiscoverer(
            schemas_dir=Path("customer_schemas"),
            samples_dir=Path("customer_samples")
        )
        schema_tables = discoverer.discover_tenant_schema("tenant_A")
        print(f"✅ Schema discovery test: found {len(schema_tables)} tables")
    else:
        print("⚠️  Sample data not found - skipping discovery test")
except Exception as e:
    print(f"❌ Discovery test failed: {e}")
"""
    
    try:
        exec(test_script)
    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        sys.exit(1)
    
    # Show next steps
    print("\n" + "=" * 50)
    print("🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Copy env.example to .env and configure (optional):")
    print("   cp env.example .env")
    print("   # Edit .env to add your OpenAI API key")
    print()
    print("2. Try the demo:")
    print("   python demo.py")
    print()
    print("3. Use the CLI:")
    print("   schema-translator list-tenants")
    print("   schema-translator discover tenant_A")
    print("   schema-translator propose tenant_A --mock")
    print()
    print("4. Start the API server:")
    print("   schema-translator serve")
    print("   # Then visit http://localhost:8000/docs")
    print()
    print("5. Run tests:")
    print("   python -m pytest tests/")
    print()
    print("For more information, see README.md")


if __name__ == "__main__":
    main()
