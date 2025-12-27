# Deployment Guide

This guide covers deploying Schema Translator to various hosting platforms.

## 🚀 Quick Deploy Options

### Option 1: Render (Recommended - Free Tier Available)

**Render** offers a free tier and is easy to set up.

#### Steps:

1. **Sign up at Render**: https://render.com
   - Use GitHub to sign in (easiest)

2. **Create New Web Service**:
   - Click "New +" → "Web Service"
   - Connect your GitHub repository: `sashankvarma2000/schema-translator`
   - Select the repository

3. **Configure Service**:
   - **Name**: `schema-translator` (or your choice)
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python web_dashboard.py`
   - **Plan**: Free (or paid if you need more resources)

4. **Environment Variables**:
   - Add `OPENAI_API_KEY`: Your OpenAI API key
   - Add `PORT`: `8080` (Render sets this automatically, but good to have)

5. **Deploy**:
   - Click "Create Web Service"
   - Wait for build to complete (~5-10 minutes)
   - Your app will be live at: `https://schema-translator.onrender.com`

#### Render Configuration (Already Included)

The `render.yaml` file is already configured. You can use it or configure manually.

---

### Option 2: Railway (Easy Setup)

**Railway** is another great option with a free tier.

#### Steps:

1. **Sign up**: https://railway.app
   - Sign in with GitHub

2. **Create New Project**:
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose `sashankvarma2000/schema-translator`

3. **Configure**:
   - Railway auto-detects Python
   - Add environment variable: `OPENAI_API_KEY`
   - Set start command: `python web_dashboard.py`

4. **Deploy**:
   - Railway will automatically deploy
   - Get your URL: `https://schema-translator.up.railway.app`

---

### Option 3: Fly.io (Global Edge Deployment)

**Fly.io** offers global deployment with a free tier.

#### Steps:

1. **Install Fly CLI**:
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```

2. **Login**:
   ```bash
   fly auth login
   ```

3. **Create App**:
   ```bash
   cd github_export
   fly launch
   ```

4. **Deploy**:
   ```bash
   fly deploy
   ```

---

### Option 4: Heroku (Classic Platform)

**Note**: Heroku removed free tier, but still popular.

#### Steps:

1. **Install Heroku CLI**: https://devcenter.heroku.com/articles/heroku-cli

2. **Login**:
   ```bash
   heroku login
   ```

3. **Create App**:
   ```bash
   cd github_export
   heroku create schema-translator
   ```

4. **Set Environment Variables**:
   ```bash
   heroku config:set OPENAI_API_KEY=your-api-key-here
   ```

5. **Deploy**:
   ```bash
   git push heroku main
   ```

---

## 🔧 Pre-Deployment Checklist

Before deploying, ensure:

- [ ] All code is committed and pushed to GitHub
- [ ] `requirements.txt` is up to date
- [ ] `Procfile` exists (for Heroku/Render)
- [ ] Environment variables are documented
- [ ] Port configuration is flexible (use `os.environ.get('PORT', 8080)`)

## 📝 Environment Variables Needed

All platforms need:

```bash
OPENAI_API_KEY=your-openai-api-key-here
PORT=8080  # Usually set automatically by platform
```

## 🛠️ Updating web_dashboard.py for Deployment

The app should use environment variable for port:

```python
import os

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)
```

## 🌐 Post-Deployment

After deployment:

1. **Test the URL**: Visit your deployment URL
2. **Check Logs**: Monitor for any errors
3. **Test Functionality**: Try a simple query translation
4. **Set up Custom Domain** (optional): Configure in platform settings

## 🔒 Security Notes

- ✅ Never commit `.env` file (already in .gitignore)
- ✅ Use platform's environment variable system
- ✅ Enable HTTPS (most platforms do this automatically)
- ✅ Set up rate limiting if needed

## 📊 Platform Comparison

| Platform | Free Tier | Ease of Setup | Recommended For |
|----------|-----------|---------------|-----------------|
| **Render** | ✅ Yes | ⭐⭐⭐⭐⭐ | Best overall |
| **Railway** | ✅ Yes | ⭐⭐⭐⭐⭐ | Quick deployment |
| **Fly.io** | ✅ Yes | ⭐⭐⭐⭐ | Global edge |
| **Heroku** | ❌ No | ⭐⭐⭐⭐ | Enterprise |
| **DigitalOcean** | ❌ No | ⭐⭐⭐ | Production |

## 🚨 Troubleshooting

### "Application Error"
- Check logs in platform dashboard
- Verify environment variables are set
- Ensure `requirements.txt` is correct

### "Port already in use"
- Use `os.environ.get('PORT', 8080)` for port
- Platform will set PORT automatically

### "Module not found"
- Check `requirements.txt` includes all dependencies
- Verify build completed successfully

---

## 🎯 Recommended: Render Deployment

**Why Render?**
- ✅ Free tier available
- ✅ Easy GitHub integration
- ✅ Automatic HTTPS
- ✅ Simple configuration
- ✅ Good documentation

**Quick Start**:
1. Go to https://render.com
2. Sign in with GitHub
3. New Web Service → Connect `schema-translator` repo
4. Add `OPENAI_API_KEY` environment variable
5. Deploy!

Your app will be live in ~10 minutes! 🚀

