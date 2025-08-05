@echo off
echo ============================================
echo Deploying TalentCentral Assistant to Render
echo ============================================

:: Step 1: Install npm dependencies
echo Installing dependencies...
call npm install
IF %ERRORLEVEL% NEQ 0 (
  echo Error installing dependencies. Aborting.
  pause
  exit /b
)

:: Step 2: Add all changes to Git
echo Adding changes to Git...
git add -A

:: Step 3: Commit changes
echo Committing...
git commit -m "Deploying to Render"

:: Step 4: Push to GitHub (triggers Render auto-deploy)
echo Pushing to GitHub...
git push origin main
IF %ERRORLEVEL% NEQ 0 (
  echo Git push failed. Check your Git config or authentication.
  pause
  exit /b
)

echo --------------------------------------------
echo Done! Render will now automatically deploy.
pause
