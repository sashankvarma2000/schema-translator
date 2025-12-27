#!/bin/bash
# Quick Start Script for GitHub Push
# Run this script after creating your GitHub repository

echo "🚀 Schema Translator - GitHub Push Helper"
echo "=========================================="
echo ""

# Check if git is installed
if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed. Please install git first."
    exit 1
fi

echo "✅ Git is installed: $(git --version)"
echo ""

# Check if already a git repository
if [ -d ".git" ]; then
    echo "⚠️  Git repository already initialized"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "📦 Initializing git repository..."
    git init
fi

echo ""
echo "📝 Adding all files..."
git add .

echo ""
echo "📋 Files to be committed:"
git status --short | head -20

echo ""
read -p "Create initial commit? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    git commit -m "Initial commit: Schema Translator - LLM-powered multi-tenant query translation system"
    echo "✅ Commit created!"
else
    echo "⏭️  Skipped commit"
    exit 0
fi

echo ""
echo "🔗 Next steps:"
echo "1. Create a repository on GitHub (if not done already)"
echo "2. Run these commands (replace YOUR_USERNAME):"
echo ""
echo "   git remote add origin https://github.com/YOUR_USERNAME/schema-translator.git"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo "📖 See GITHUB_PUSH_STEPS.md for detailed instructions"
