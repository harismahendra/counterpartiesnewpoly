# Dome API Frontend

Modern frontend for displaying Dome API order updates in real-time.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start development server:
```bash
npm run dev
```

The frontend will run on `http://localhost:3000`

## Build for Production

```bash
npm run build
```

## Features

- Real-time order updates via Socket.IO
- Beautiful, responsive UI
- Full-width table with horizontal scroll
- Millisecond-precision timestamps
- Statistics dashboard
- Color-coded BUY/SELL badges

## Configuration

The frontend connects to the backend at `http://localhost:8000` by default. Make sure the backend is running before starting the frontend.
