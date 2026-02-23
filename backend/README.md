# Dome API Backend (Python)

Python backend for Dome API WebSocket connection.

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
