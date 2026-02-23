# Dome API WebSocket Connector

A split architecture application with Python backend and modern frontend for tracking wallet orders on Dome API (Polymarket/Kalshi).

## Architecture

- **Backend (Python)**: Handles WebSocket connection to Dome API, processes orders, and broadcasts to frontend
- **Frontend (Vite + Vanilla JS)**: Beautiful, modern UI for displaying real-time order updates

## Quick Start

### 1. Backend Setup

```bash
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your API key and wallet addresses
python main.py
```

Backend runs on `http://localhost:8000`

### 2. Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on `http://localhost:3000`

## Project Structure

```
WSDome/
├── backend/           # Python backend
│   ├── main.py       # FastAPI + Socket.IO server
│   ├── requirements.txt
│   └── .env.example
├── frontend/         # Vite frontend
│   ├── src/
│   │   ├── main.js   # Frontend logic
│   │   └── style.css # Styles
│   ├── index.html
│   └── package.json
└── README.md
```

## Features

- 🔌 Real-time WebSocket connection to Dome API
- 👛 Track orders from multiple wallet addresses
- 📊 Beautiful, responsive UI with real-time updates
- ⚙️ Configuration via environment variables
- 🔄 Automatic reconnection
- 📈 Statistics dashboard

## Configuration

### Backend (.env)

- `DOME_API_KEY` (required): Your Dome API key
- `WALLET_1_ADDRESS`, `WALLET_2_ADDRESS`, etc.: Wallet addresses to track
- `PLATFORM`: `polymarket` or `kalshi` (default: `polymarket`)
- `BACKEND_PORT`: Backend server port (default: `8000`)
- `FRONTEND_PORT`: Frontend server port (default: `3000`)

### Frontend

No configuration needed - connects to backend automatically.

## Development

### Backend
```bash
cd backend
uvicorn main:socketio_app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm run dev
```

## Production

### Backend
```bash
cd backend
uvicorn main:socketio_app --host 0.0.0.0 --port 8000
```

### Frontend
```bash
cd frontend
npm run build
npm run preview
```

## Deployment

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed instructions on deploying to GitHub and Render.

### Quick Deploy to Render

1. Push your code to GitHub
2. Create a new Web Service in Render for the backend
3. Create a new Static Site in Render for the frontend
4. Set environment variables as described in `DEPLOYMENT.md`

## License

MIT
