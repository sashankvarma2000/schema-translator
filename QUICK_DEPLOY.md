# 🚀 Quick Deploy to Render (Recommended)

## Fastest Way to Get Your App Live

### Step 1: Sign Up
1. Go to **https://render.com**
2. Click **"Get Started for Free"**
3. Sign in with **GitHub** (easiest option)

### Step 2: Deploy
1. Click **"New +"** → **"Web Service"**
2. Connect your GitHub account (if not already)
3. Select repository: **`sashankvarma2000/schema-translator`**
4. Click **"Connect"**

### Step 3: Configure
- **Name**: `schema-translator` (or your choice)
- **Region**: Choose closest to you
- **Branch**: `main`
- **Root Directory**: Leave empty (or `github_export` if you want)
- **Environment**: `Python 3`
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `python web_dashboard.py`
- **Plan**: **Free** (or upgrade if needed)

### Step 4: Environment Variables
Click **"Advanced"** → **"Add Environment Variable"**:
- **Key**: `OPENAI_API_KEY`
- **Value**: Your OpenAI API key
- Click **"Add"**

### Step 5: Deploy!
1. Scroll down and click **"Create Web Service"**
2. Wait for build (~5-10 minutes)
3. Your app will be live at: **`https://schema-translator.onrender.com`**

## ✅ Done!

Your app is now live and accessible worldwide!

**Note**: Free tier apps spin down after 15 minutes of inactivity. First request may take ~30 seconds to wake up.

---

## Alternative: Railway (Also Free)

1. Go to **https://railway.app**
2. Sign in with GitHub
3. **New Project** → **Deploy from GitHub repo**
4. Select `schema-translator`
5. Add environment variable: `OPENAI_API_KEY`
6. Deploy automatically!

---

## Need Help?

Check `DEPLOYMENT_GUIDE.md` for detailed instructions and troubleshooting.

