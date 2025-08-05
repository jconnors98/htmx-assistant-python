@echo off
echo ============================================
echo Deploying TalentCentral Assistant to Render
echo ============================================

:: Step 1: Install dependencies
echo  Installing dependencies...
npm install

:: Step 2: Add and commit changes
echo  Committing changes...
git add .
git commit -m "Deploying to Render"

:: Step 3: Push to GitHub (triggers Render auto-deploy)
echo  Pushing to GitHub...
git push origin main

:: Step 4: Done
echo ✅ Deployment triggered. Render will rebuild and deploy automatically.
pause