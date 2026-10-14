# TFG lobby server

Lobby + relay server for The Fighting Grounds online lobby mod (tfg_online.lua + tfg_online.exe).
Players open a lobby by name and password, friends join by name, and the game traffic is relayed
through this server, so nobody types an IP address or opens router ports.

Runs anywhere Go runs: `go build -o tfg-lobby . && ./tfg-lobby` (listens on $PORT, default 8080).
`render.yaml` deploys it as a free Render web service.

- `GET /`        "TFG lobby server is running."
- `GET /health`  ok
- `GET /lobbies` open lobbies (no passwords)
- `/ws`          WebSocket used by tfg_online.exe
