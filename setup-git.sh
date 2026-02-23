#!/bin/bash
# Setup script for initializing git repository and preparing for GitHub deployment

echo "🚀 Setting up Git repository for WSDome..."

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo "📦 Initializing git repository..."
    git init
    git branch -M main
else
    echo "✅ Git repository already initialized"
fi

# Check if .env files exist and warn
if [ -f "backend/.env" ] || [ -f ".env" ]; then
    echo "⚠️  WARNING: .env files detected. These will NOT be committed (they're in .gitignore)"
    echo "   Make sure to set environment variables in Render dashboard!"
fi

# Add all files
echo "📝 Adding files to git..."
git add .

# Show status
echo ""
echo "📊 Git status:"
git status --short

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Create a new repository on GitHub (https://github.com/new)"
echo "2. Run these commands:"
echo "   git remote add origin https://github.com/YOUR_USERNAME/REPO_NAME.git"
echo "   git commit -m 'Initial commit: WSDome application'"
echo "   git push -u origin main"
echo ""
echo "3. Then follow the deployment guide in DEPLOYMENT.md"
