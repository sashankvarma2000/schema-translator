# Render Start Command - Exact Format

## ✅ Correct Start Command for Render

When Render asks for the **Start Command**, use:

```
gunicorn web_dashboard:app
```

## Format Explanation

The format is: `gunicorn <module>:<variable>`

- **`web_dashboard`** = The Python file name (`web_dashboard.py` without `.py`)
- **`app`** = The Flask application instance variable name (defined as `app = Flask(__name__)`)

## Alternative (with port binding)

You can also use:

```
gunicorn --bind 0.0.0.0:$PORT web_dashboard:app
```

But the simpler version works fine since Render sets `$PORT` automatically.

## Render Configuration

**Build Command**:
```
pip install -r requirements.txt
```

**Start Command**:
```
gunicorn web_dashboard:app
```

**Environment Variables**:
- `OPENAI_API_KEY` = your OpenAI API key

## Why This Works

1. Gunicorn is a WSGI HTTP Server for Python
2. It imports the `app` object from `web_dashboard.py`
3. It serves the Flask application on the port Render provides
4. This is the production-ready way to run Flask apps

## Testing Locally

To test this command locally:

```bash
# Install gunicorn
pip install gunicorn

# Run the app
gunicorn web_dashboard:app --bind 0.0.0.0:8080
```

Then visit: http://localhost:8080

## ✅ That's It!

Just use: `gunicorn web_dashboard:app` in Render's Start Command field.

