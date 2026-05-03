# Predexon Streaming Backend (Python)

Python backend for Predexon WebSocket streaming and frontend Socket.IO broadcasting.

## Setup

1. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```

4. Run the backend:
```bash
python main.py
```

Or with uvicorn directly:
```bash
uvicorn main:app --reload --port 8000
```

## Environment Variables

See `.env.example` for all available configuration options.

Required for streaming:

- `PREDEXON_API_KEY` (preferred)
- `DOME_API_KEY` (legacy fallback; kept for backward compatibility)

The backend subscribes to Predexon `orders` events using configured wallet addresses.

## API Endpoints

- `GET /` - Health check and status
- `GET /health` - Health check

## WebSocket Events

The backend uses Socket.IO to communicate with the frontend:

- **Emitted events:**
  - `status` - Connection status updates
  - `order-update` - Real-time order updates

- **Received events:**
  - `connect` - Frontend connection
  - `disconnect` - Frontend disconnection
