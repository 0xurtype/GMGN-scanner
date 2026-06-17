# GMGN Signal Scanner

Token scanner & wallet tracker for Solana memecoins. Single-file app with Docker deployment.

## Features

- **Token Scanner** — real-time feed with KOL buys, smart wallets, snipers, holder count
- **Trending** — hot tokens ranked by volume, price action, holder growth
- **Smart Money Tracker** — whale and sniper wallet activity with PnL
- **KOL Activity** — influencer calls, hit rates, recent trades
- **Wallet Tracker** — track any Solana wallet, view trades and PnL
- **Signal Filters** — KOL buys, smart wallets, snipers, dev checks, safety filters

## Quick Start

### Docker (recommended)

```bash
docker compose up -d --build
# → http://localhost
```

### Node (local dev)

```bash
npm run dev
# → http://localhost:3000
```

### Static hosting

Serve `public/` with any HTTP server:

```bash
npx serve public -l 3000
# or
python3 -m http.server 3000 --directory public
```

## Deploy to VPS

### Docker

```bash
ssh user@your-vps-ip
git clone https://github.com/0xurtype/GMGN-scanner.git
cd GMGN-scanner
docker compose up -d --build
```

### Bare Nginx

```bash
sudo apt install nginx -y
git clone https://github.com/0xurtype/GMGN-scanner.git
sudo cp -r GMGN-scanner/public/* /var/www/html/
sudo systemctl restart nginx
```

## Project Structure

```
GMGN-scanner/
├── public/
│   └── index.html          Single-file app (HTML + CSS + JS)
├── package.json
├── Dockerfile              Nginx + Alpine container
├── nginx.conf              Nginx config with security headers
├── docker-compose.yaml     One-command deploy
└── .gitignore
```

## Roadmap

- [ ] Replace mock data with real API calls (Helius, Birdeye, Jupiter)
- [ ] Wallet authentication (Phantom / Solflare)
- [ ] Backend service (Node/Go + PostgreSQL) for wallet persistence
- [ ] WebSocket real-time feed
- [ ] Rate limiting & caching layer
- [ ] Auth & billing

## License

MIT
