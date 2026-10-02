# Varroc Live Material Control

Actual online multi-device live-sync version.

## Run
1. Install Node.js 18+.
2. In this folder run:
   npm install
   npm start
3. Open http://localhost:3000

## Important
For two different phones/computers on the internet, this server must be deployed to a public Node.js host that supports WebSockets/Socket.IO and persistent storage. Opening localhost on two devices will NOT make them share data.

## Included
- Create Room / Join Room
- Realtime Socket.IO synchronization
- 506 / 507 totals
- Shared history
- Delete entries
- Owner can close room
- Server-side JSON persistence

## Important for the Add button
Do not double-click public/index.html for the live version. Run `npm install` and `npm start`, then open `http://localhost:3000`. The page needs Socket.IO from the Node server for realtime rooms. The updated HTML also supports local testing when opened directly.
