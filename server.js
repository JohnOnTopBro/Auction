import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";

const PORT = Number(process.env.PORT || 8080);
const httpServer = createServer((req, res) => {
  res.writeHead(200, {"Content-Type": "text/plain; charset=utf-8"});
  res.end("TikTok Auction Gift Bridge is running.");
});

const wss = new WebSocketServer({ server: httpServer });
const clients = new Set();
let tiktok = null;
let currentUsername = "";

function broadcast(payload) {
  const text = JSON.stringify(payload);
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(text);
  }
}

wss.on("connection", ws => {
  clients.add(ws);
  ws.send(JSON.stringify({type:"status", message:"Bridge connected. Waiting for TikTok username..." }));

  ws.on("message", async raw => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type !== "connect") return;

      const username = String(msg.username || "").replace(/^@/, "").trim();
      if (!username) {
        ws.send(JSON.stringify({type:"status", message:"Enter a TikTok LIVE username."}));
        return;
      }

      currentUsername = username;

      if (tiktok) {
        try { await tiktok.disconnect(); } catch {}
        tiktok = null;
      }

      ws.send(JSON.stringify({type:"status", message:`Connecting to @${username}...`}));

      tiktok = new TikTokLiveConnection(username, {
        processInitialData: false,
        enableExtendedGiftInfo: true,
        requestPollingIntervalMs: 1000
      });

      tiktok.on(ControlEvent.CONNECTED, state => {
        broadcast({
          type:"status",
          message:`TikTok LIVE connected: @${username}`
        });
      });

      tiktok.on(ControlEvent.DISCONNECTED, () => {
        broadcast({
          type:"status",
          message:`TikTok LIVE disconnected: @${username}`
        });
      });

      tiktok.on(ControlEvent.ERROR, err => {
        broadcast({
          type:"status",
          message:`TikTok error: ${err?.message || err}`
        });
      });

      tiktok.on(WebcastEvent.GIFT, data => {
        // Streakable gifts emit intermediate events and a final repeatEnd event.
        // Only process the final event so the leaderboard doesn't double-count.
        if (Number(data.giftType) === 1 && !data.repeatEnd) return;

        const repeatCount = Math.max(1, Number(data.repeatCount || 1));
        const diamondCount = Math.max(
          0,
          Number(
            data.diamondCount ??
            data.extendedGiftInfo?.diamond_count ??
            data.extendedGiftInfo?.diamondCount ??
            0
          )
        );

        const username = data.uniqueId || data.user?.uniqueId || "Unknown Viewer";
        const nickname = data.nickname || data.user?.nickname || username;
        const giftName = data.giftName || data.extendedGiftInfo?.name || "Gift";

        broadcast({
          type: "gift",
          username,
          nickname,
          giftName,
          giftId: data.giftId ?? data.gift?.gift_id ?? null,
          diamondCount,
          repeatCount,
          timestamp: Date.now()
        });

        console.log(
          `🎁 ${username} sent ${giftName} x${repeatCount} (${diamondCount} diamonds each)`
        );
      });

      try {
        await tiktok.connect();
      } catch (err) {
        broadcast({
          type:"status",
          message:`Could not connect: ${err?.message || err}`
        });
      }
    } catch (err) {
      ws.send(JSON.stringify({
        type:"status",
        message:`Invalid bridge message: ${err?.message || err}`
      }));
    }
  });

  ws.on("close", () => clients.delete(ws));
});

httpServer.listen(PORT, () => {
  console.log(`Auction bridge running on http://localhost:${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
});
