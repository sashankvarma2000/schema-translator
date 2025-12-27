# Step-by-Step Guide: Push to GitHub

## Prerequisites

1. **GitHub Account**: Make sure you have a GitHub account (sign up at https://github.com)
2. **Git Installed**: Check if git is installed:
   ```bash
   git --version
   ```
   If not installed, download from: https://git-scm.com/downloads

3. **GitHub Authentication**: 
   - Option A: Use HTTPS with Personal Access Token (recommended)
   - Option B: Use SSH keys (if already set up)

---

## Step 1: Create GitHub Repository

1. Go to https://github.com and sign in
2. Click the **"+"** icon in the top right corner
3. Select **"New repository"**
4. Fill in the details:
   - **Repository name**: `schema-translator` (or your preferred name)
   - **Description**: `LLM-powered multi-tenant query translation system`
   - **Visibility**: Choose **Public** or **Private**
   - **⚠️ IMPORTANT**: 
     - ❌ **DO NOT** check "Add a README file"
     - ❌ **DO NOT** check "Add .gitignore"
     - ❌ **DO NOT** check "Choose a license"
     - (We already have these files!)
5. Click **"Create repository"**

---

## Step 2: Navigate to Export Directory

Open your terminal and navigate to the export directory:

```bash
cd /Users/sashank/Desktop/Sashank/girish/github_export
```

Verify you're in the right place:
```bash
pwd
# Should show: /Users/sashank/Desktop/Sashank/girish/github_export

ls -la
# Should show README.md, .gitignore, src/, etc.
```

---

## Step 3: Initialize Git Repository

```bash
# Initialize git repository
git init

# Verify it worked
ls -la .git
# Should show git directory
```

---

## Step 4: Configure Git (if first time)

If this is your first time using git on this computer:

```bash
# Set your name (use your GitHub username or real name)
git config --global user.name "Your Name"

# Set your email (use your GitHub email)
git config --global user.email "your.email@example.com"

# Verify configuration
git config --list
```

---

## Step 5: Add All Files

```bash
# Add all files to staging
git add .

# Check what will be committed
git status
# Should show all files as "new file" or "Changes to be committed"
```

**Expected output**: You should see files like:
- README.md
- LICENSE
- .gitignore
- src/
- config/
- etc.

**⚠️ Important**: Make sure `.env` file is NOT listed (it should be ignored by .gitignore)

---

## Step 6: Create Initial Commit

```bash
# Create the first commit
git commit -m "Initial commit: Schema Translator - LLM-powered multi-tenant query translation system"

# Verify commit was created
git log
# Should show your commit
```

---

## Step 7: Connect to GitHub Repository

After creating the repository on GitHub, you'll see a page with setup instructions. Copy the repository URL.

**Option A: HTTPS (Recommended for beginners)**

```bash
# Replace YOUR_USERNAME with your GitHub username
git remote add origin https://github.com/YOUR_USERNAME/schema-translator.git

# Verify remote was added
git remote -v
# Should show:
# origin  https://github.com/YOUR_USERNAME/schema-translator.git (fetch)
# origin  https://github.com/YOUR_USERNAME/schema-translator.git (push)
```

**Option B: SSH (If you have SSH keys set up)**

```bash
git remote add origin git@github.com:YOUR_USERNAME/schema-translator.git
```

---

## Step 8: Rename Branch to Main (if needed)

```bash
# Check current branch name
git branch

# If it shows "master", rename to "main"
git branch -M main

# Verify
git branch
# Should show: * main
```

---

## Step 9: Push to GitHub

### For HTTPS (will prompt for credentials):

```bash
# Push to GitHub
git push -u origin main
```

**If prompted for credentials:**
- **Username**: Your GitHub username
- **Password**: Use a **Personal Access Token** (NOT your GitHub password)

**To create a Personal Access Token:**
1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Give it a name (e.g., "schema-translator-push")
4. Select scopes: Check **"repo"** (full control of private repositories)
5. Click "Generate token"
6. **Copy the token immediately** (you won't see it again!)
7. Use this token as your password when pushing

### For SSH:

```bash
git push -u origin main
```

---

## Step 10: Verify Upload

1. Go to your GitHub repository page: `https://github.com/YOUR_USERNAME/schema-translator`
2. You should see:
   - ✅ All files uploaded
   - ✅ README.md displays correctly
   - ✅ File count matches (85+ files)
   - ✅ No cache files visible (thanks to .gitignore)

---

## Troubleshooting

### Issue: "remote origin already exists"
```bash
# Remove existing remote
git remote remove origin

# Add it again
git remote add origin https://github.com/YOUR_USERNAME/schema-translator.git
```

### Issue: "Authentication failed"
- Make sure you're using a Personal Access Token (not password)
- Token must have "repo" scope
- For SSH: Make sure your SSH key is added to GitHub

### Issue: "Permission denied"
- Check repository name matches
- Verify you have write access to the repository
- Make sure you're using the correct username

### Issue: "Large files" warning
- The export is ~1.8MB, which is fine
- If you get warnings about large files, they're likely in customer_samples/ which should be excluded by .gitignore

### Issue: "Branch 'main' has no upstream branch"
```bash
# Set upstream branch
git push -u origin main
```

---

## Quick Reference Commands

```bash
# Navigate to export directory
cd /Users/sashank/Desktop/Sashank/girish/github_export

# Initialize git
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: Schema Translator"

# Add remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/schema-translator.git

# Push to GitHub
git push -u origin main
```

---

## After Successful Push

1. **Add Repository Description**: 
   - Go to repository → Settings → General
   - Add description: "LLM-powered multi-tenant query translation system"

2. **Add Topics/Tags**:
   - Click the gear icon next to "About"
   - Add topics: `python`, `llm`, `sql`, `schema-translation`, `multi-tenant`, `openai`, `flask`, `duckdb`

3. **Verify Files**:
   - Check that README.md renders correctly
   - Verify .gitignore is working (no __pycache__ visible)
   - Check that LICENSE is recognized

4. **Create Release** (Optional):
   - Go to Releases → Create a new release
   - Tag: `v1.0.0`
   - Title: `Schema Translator v1.0.0`
   - Description: "Initial release of Schema Translator"

---

## Next Steps

After pushing:
- ✅ Share the repository link
- ✅ Add collaborators if needed
- ✅ Set up GitHub Actions for CI/CD (optional)
- ✅ Enable GitHub Pages for documentation (optional)
- ✅ Create issues for bugs/features
- ✅ Add a code of conduct (optional)

---

## Security Reminders

⚠️ **IMPORTANT**: 
- Never commit `.env` file (it contains API keys)
- Never commit database files (`.duckdb`)
- Never commit cache files
- The `.gitignore` file should prevent these, but always double-check with `git status` before committing

---

## Success Checklist

- [ ] GitHub repository created
- [ ] Git initialized in export directory
- [ ] All files added and committed
- [ ] Remote repository connected
- [ ] Code pushed to GitHub
- [ ] Files visible on GitHub
- [ ] README.md displays correctly
- [ ] No sensitive files (`.env`, cache) visible

---

**Need Help?** 
- GitHub Docs: https://docs.github.com
- Git Docs: https://git-scm.com/doc
- Create an issue in the repository for questions

