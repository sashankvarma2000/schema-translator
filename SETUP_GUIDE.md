# 🚀 Schema Translator Setup Guide

## For Running on Another Laptop

### ✅ **Step 1: Install Dependencies**

```bash
# Install Python 3.11+ (if not already installed)
python --version

# Install required packages
pip install flask==2.3.3
pip install duckdb==1.4.0
pip install openai==1.108.1
pip install pandas==2.1.0
pip install pyyaml==6.0.1
pip install requests==2.31.0
pip install rich==13.7.0
pip install python-dotenv==1.0.0
```

### ✅ **Step 2: Set Environment Variables**

Create a `.env` file in the project root:

```bash
# .env file
OPENAI_API_KEY=your_openai_api_key_here
```

**OR** set environment variable:
```bash
export OPENAI_API_KEY="your_openai_api_key_here"
```

### ✅ **Step 3: Copy Database Files**

**CRITICAL**: The `databases/` folder with DuckDB files must be copied:

```bash
# Copy the entire databases folder
cp -r /path/to/source/databases/ /path/to/destination/databases/

# Verify databases exist
ls -la databases/
# Should show: tenant_A.duckdb, tenant_B.duckdb, tenant_C.duckdb, tenant_D.duckdb, tenant_E.duckdb, tenant_F.duckdb
```

### ✅ **Step 4: Copy Cache Files**

**IMPORTANT**: Copy the mapping cache for faster startup:

```bash
# Copy cache folder
cp -r /path/to/source/cache/ /path/to/destination/cache/

# Verify cache exists
ls -la cache/
# Should show: mapping_cache.json
```

### ✅ **Step 5: Test the Setup**

```bash
# 1. Test database access
python -c "
import duckdb
conn = duckdb.connect('databases/tenant_A.duckdb', read_only=True)
result = conn.execute('SELECT COUNT(*) FROM transactions').fetchone()
print(f'tenant_A has {result[0]} records')
conn.close()
"

# 2. Test OpenAI API
python -c "
import os
from openai import OpenAI
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
print('OpenAI API key is set:', bool(client.api_key))
"

# 3. Start the server
python web_dashboard.py
```

### ✅ **Step 6: Verify Everything Works**

Open browser: http://localhost:8080

**Test these queries:**

1. **Natural Language Tab**:
   - Select: tenant_A
   - Query: "What are the top 10 highest value contracts?"
   - Click: "Generate & Execute"

2. **Query Translation Tab**:
   - Select: tenant_D  
   - SQL: `SELECT contract_id, value_amount FROM contracts WHERE value_amount > 1000000 ORDER BY value_amount DESC LIMIT 10;`
   - Click: "Translate & Execute"

---

## 🚨 **Common Issues & Solutions**

### **Issue 1: "ModuleNotFoundError"**
```bash
# Solution: Install missing packages
pip install flask duckdb openai pandas pyyaml requests rich python-dotenv
```

### **Issue 2: "Database not found"**
```bash
# Solution: Copy databases folder
cp -r /source/databases/ ./databases/
```

### **Issue 3: "OpenAI API key not found"**
```bash
# Solution: Set environment variable
export OPENAI_API_KEY="your_key_here"
# OR create .env file with OPENAI_API_KEY=your_key_here
```

### **Issue 4: "Port 8080 already in use"**
```bash
# Solution: Kill existing process
pkill -9 -f web_dashboard.py
# OR use different port
python web_dashboard.py --port 8081
```

### **Issue 5: "No results returned"**
```bash
# Check if databases have data
python -c "
import duckdb
for tenant in ['tenant_A', 'tenant_B', 'tenant_C', 'tenant_D']:
    try:
        conn = duckdb.connect(f'databases/{tenant}.duckdb', read_only=True)
        tables = conn.execute('SHOW TABLES').fetchall()
        print(f'{tenant}: {len(tables)} tables')
        conn.close()
    except Exception as e:
        print(f'{tenant}: ERROR - {e}')
"
```

---

## 📋 **Quick Checklist**

- [ ] Python 3.11+ installed
- [ ] All dependencies installed (`pip install -r requirements.txt`)
- [ ] OpenAI API key set (`export OPENAI_API_KEY="..."`)
- [ ] `databases/` folder copied with all .duckdb files
- [ ] `cache/` folder copied with mapping_cache.json
- [ ] Server starts without errors (`python web_dashboard.py`)
- [ ] Browser opens http://localhost:8080
- [ ] Natural Language query works
- [ ] SQL Translation query works

---

## 🎯 **Demo-Ready Commands**

```bash
# 1. Start server
python web_dashboard.py

# 2. Test in browser
open http://localhost:8080

# 3. Quick test via API
curl -X POST http://localhost:8080/api/query-execution/translate-and-execute \
  -H "Content-Type: application/json" \
  -d '{"canonical_query": "SELECT COUNT(*) FROM contracts", "tenant_id": "tenant_A"}'
```

---

## 📞 **If Still Having Issues**

1. **Check server logs**: Look at `server.log` file
2. **Test individual components**:
   ```bash
   # Test DuckDB
   python -c "import duckdb; print('DuckDB works')"
   
   # Test OpenAI
   python -c "from openai import OpenAI; print('OpenAI works')"
   
   # Test Flask
   python -c "from flask import Flask; print('Flask works')"
   ```

3. **Verify file permissions**:
   ```bash
   ls -la databases/
   ls -la cache/
   ```

4. **Check Python path**:
   ```bash
   python -c "import sys; print(sys.path)"
   ```

---

**Last Updated**: September 30, 2025  
**Status**: ✅ Production Ready  
**Tested On**: macOS, Python 3.11+

**Good luck with your demo!** 🚀






