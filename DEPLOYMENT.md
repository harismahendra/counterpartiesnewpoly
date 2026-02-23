# Deployment Guide

This guide will help you deploy the WSDome application to GitHub and Render.

## Prerequisites

- GitHub account
- Render account (sign up at https://render.com)
- Dome API key
- Polymarket database connection string (if using BBO matching)

## Step 1: Prepare Repository

### 1.1 Initialize Git (if not already done)

```bash
cd /Users/harisaisyah/Desktop/Polymarket/WSDome
git init
git add .
git commit -m "Initial commit: WSDome application"
```

### 1.2 Create GitHub Repository

1. Go to https://github.com/new
2. Create a new repository (e.g., `ws-dome`)
3. **Do NOT initialize with README, .gitignore, or license** (we already have these)
4. Copy the repository URL

### 1.3 Push to GitHub

```bash
git remote add origin https://github.com/YOUR_USERNAME/ws-dome.git
git branch -M main
git push -u origin main
```

## Step 2: Deploy to Render

### 2.1 Create Backend Service

1. Go to https://dashboard.render.com
2. Click **"New +"** → **"Web Service"**
3. Connect your GitHub repository
4. Configure the service:
   - **Name**: `ws-dome-backend`
   - **Environment**: `Python 3`
   - **Build Command**: `cd backend && pip install -r requirements.txt`
   - **Start Command**: `cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Plan**: Choose a plan (Starter is fine for testing)

5. Add Environment Variables:
   - `DOME_API_KEY`: Your Dome API key
   - `PLATFORM`: `polymarket` (or `kalshi`)
   - `WS_VERSION`: `1`
   - `DATABASE_URL_POLY`: Your PostgreSQL connection string (if using BBO matching)
   - `WALLET_1_ADDRESS`: First wallet address
   - `WALLET_2_ADDRESS`: Second wallet address (add more as needed)
   - Or use `WALLET_ADDRESSES`: Comma-separated list of addresses
   - `FRONTEND_URL`: Will be set after frontend is deployed (e.g., `https://ws-dome-frontend.onrender.com`)

6. Click **"Create Web Service"**

7. **Important**: Copy the backend URL (e.g., `https://ws-dome-backend.onrender.com`)

### 2.2 Create Frontend Service

1. In Render dashboard, click **"New +"** → **"Static Site"**
2. Connect the same GitHub repository
3. Configure the service:
   - **Name**: `ws-dome-frontend`
   - **Build Command**: `cd frontend && npm install && npm run build`
   - **Publish Directory**: `frontend/dist`
   - **Plan**: Choose a plan

4. Add Environment Variables:
   - `VITE_BACKEND_URL`: The backend URL from step 2.1 (e.g., `https://ws-dome-backend.onrender.com`)

5. Click **"Create Static Site"**

6. Copy the frontend URL (e.g., `https://ws-dome-frontend.onrender.com`)

### 2.3 Update Backend CORS

1. Go back to your backend service in Render
2. Add/Update environment variable:
   - `FRONTEND_URL`: The frontend URL from step 2.2 (e.g., `https://ws-dome-frontend.onrender.com`)
3. Render will automatically redeploy

## Step 3: Verify Deployment

1. Visit your frontend URL (e.g., `https://ws-dome-frontend.onrender.com`)
2. Check the browser console for any connection errors
3. Verify that orders are being received in real-time

## Troubleshooting

### Backend Issues

- **Check logs**: In Render dashboard, go to your backend service → "Logs" tab
- **Database connection**: Ensure `DATABASE_URL_POLY` is correct and includes SSL mode
- **WebSocket connection**: Check that `DOME_API_KEY` is valid

### Frontend Issues

- **CORS errors**: Ensure `FRONTEND_URL` is set correctly in backend environment variables
- **Connection refused**: Check that `VITE_BACKEND_URL` points to the correct backend URL
- **Build errors**: Check the build logs in Render dashboard

### Common Issues

1. **"Module not found"**: Ensure all dependencies are in `requirements.txt` (backend) or `package.json` (frontend)
2. **"Port already in use"**: Render automatically sets `$PORT`, don't hardcode port numbers
3. **Environment variables not loading**: Ensure variables are set in Render dashboard, not in `.env` files (which are gitignored)

## Using render.yaml (Alternative Method)

If you prefer infrastructure-as-code:

1. The `render.yaml` file is already in the repository
2. In Render dashboard, go to **"New +"** → **"Blueprint"**
3. Connect your GitHub repository
4. Render will automatically detect `render.yaml` and create services
5. You'll still need to set environment variables in the Render dashboard

## Updating Deployment

After making changes:

```bash
git add .
git commit -m "Your commit message"
git push origin main
```

Render will automatically detect the push and redeploy both services.

## Cost Considerations

- **Free tier**: Both services will spin down after 15 minutes of inactivity
- **Starter plan**: Services stay running 24/7 (~$7/month per service)
- Consider using the free tier for testing and upgrading when needed

## Security Notes

- Never commit `.env` files to Git
- Use Render's environment variables for sensitive data
- Consider using Render's secrets management for production
- Enable HTTPS (automatically enabled on Render)
