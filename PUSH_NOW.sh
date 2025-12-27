#!/bin/bash
# Push to GitHub Repository
# Repository: https://github.com/sashankvarma2000/schema-translator.git

echo "🚀 Pushing Schema Translator to GitHub"
echo "======================================"
echo ""
echo "Repository: https://github.com/sashankvarma2000/schema-translator.git"
echo ""

# Check if git is installed
if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed. Please install git first."
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "README.md" ]; then
    echo "❌ README.md not found. Make sure you're in the github_export directory."
    exit 1
fi

# Initialize git if needed
if [ ! -d ".git" ]; then
    echo "📦 Initializing git repository..."
    git init
fi

# Add remote
echo "🔗 Configuring remote repository..."
git remote remove origin 2>/dev/null
git remote add origin https://github.com/sashankvarma2000/schema-translator.git

# Set branch to main
git branch -M main

# Add all files
echo "📝 Adding files..."
git add .

# Check status
echo ""
echo "📋 Files to be committed:"
git status --short | head -20
echo ""

# Create commit
echo "💾 Creating commit..."
git commit -m "Initial commit: Schema Translator - LLM-powered multi-tenant query translation system" || {
    echo "⚠️  Commit failed or nothing to commit"
}

echo ""
echo "🚀 Ready to push!"
echo ""
echo "Next step: Run this command to push to GitHub:"
echo ""
echo "   git push -u origin main"
echo ""
echo "⚠️  You'll be prompted for credentials:"
echo "   - Username: sashankvarma2000"
echo "   - Password: Use a Personal Access Token (not your GitHub password)"
echo ""
echo "To create a token:"
echo "   GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)"
echo "   → Generate new token → Check 'repo' scope"
echo ""

