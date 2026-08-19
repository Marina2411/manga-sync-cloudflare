import { DurableObject } from "cloudflare:workers";

const MAX_NAME = 80;
const MAX_ROOM = 120;
const MAX_CHAT = 2000;

function cleanString(value, max) {
  return String(value ?? "").trim().slice(0, max);
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("Manga Sync Cloudflare server is running", {
        headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" },
      });
    }

    if (url.pathname !== "/ws") {
      return jsonResponse({ ok: false, error: "Not found" }, 404);
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return jsonResponse({ ok: false, error: "WebSocket upgrade required" }, 426);
    }

    const roomID = cleanString(url.searchParams.get("room"), MAX_ROOM);
    if (!roomID) {
      return jsonResponse({ ok: false, error: "Room is required" }, 400);
    }

    // One Durable Object = one manga room.
    const id = env.MANGA_ROOMS.idFromName(roomID);
    const room = env.MANGA_ROOMS.get(id);

    return room.fetch(`https://room.internal/websocket?room=${encodeURIComponent(roomID)}`, request);
  },
};

export class MangaRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname !== "/websocket") {
      return new Response("Not found", { status: 404 });
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Hibernatable WebSocket: the room can sleep without disconnecting users.
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      name: "",
      chapter: "",
      isLeader: false,
      url: "",
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  getClients() {
    return this.ctx.getWebSockets();
  }

  getAttachment(ws) {
    try {
      return ws.deserializeAttachment() || {};
    } catch (_) {
      return {};
    }
  }

  setAttachment(ws, patch) {
    const old = this.getAttachment(ws);
    ws.serializeAttachment({ ...old, ...patch });
  }

  send(ws, data) {
    if (ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(data));
      return true;
    } catch (_) {
      return false;
    }
  }

  broadcast(data, except = null) {
    for (const ws of this.getClients()) {
      if (ws !== except) this.send(ws, data);
    }
  }

  currentLeader() {
    for (const ws of this.getClients()) {
      const a = this.getAttachment(ws);
      if (a.isLeader && a.name) return { ws, ...a };
    }
    return null;
  }

  sendRoomState(ws) {
    const participants = [];

    for (const client of this.getClients()) {
      const a = this.getAttachment(client);
      if (client !== ws && a.name) {
        participants.push({
          sender: a.name,
          chapter: a.chapter || "",
        });
      }
    }

    const leader = this.currentLeader();

    this.send(ws, {
      type: "room_state",
      leaderName: leader?.name || null,
      leaderUrl: leader?.url || null,
      leaderChapter: leader?.chapter || "",
      participants,
    });
  }

  webSocketMessage(ws, message) {
    let data;
    try {
      data = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message));
    } catch (_) {
      return;
    }

    const type = cleanString(data.type, 40);

    if (type === "heartbeat") {
      this.send(ws, { type: "heartbeat_ack", ts: Date.now() });
      return;
    }

    if (type === "join") {
      const name = cleanString(data.sender, MAX_NAME);
      const chapter = cleanString(data.chapter, 300);
      if (!name) return;

      this.setAttachment(ws, {
        name,
        chapter,
        isLeader: false,
        url: "",
      });

      this.sendRoomState(ws);

      this.broadcast({
        type: "participant",
        sender: name,
        chapter,
      }, ws);
      return;
    }

    const me = this.getAttachment(ws);
    if (!me.name) return;

    if (type === "claim_leader") {
      for (const client of this.getClients()) {
        const a = this.getAttachment(client);
        if (a.name) {
          this.setAttachment(client, { isLeader: client === ws });
        }
      }

      this.sendRoomState(ws);
      this.broadcast({
        type: "leader_changed",
        leaderName: me.name,
      });
      return;
    }

    if (type === "leader_url") {
      if (!me.isLeader) return;

      const url = cleanString(data.url, 2000);
      const chapter = cleanString(data.chapter || me.chapter, 300);

      this.setAttachment(ws, { url, chapter });

      this.broadcast({
        type: "leader_url",
        url,
        sender: me.name,
        chapter,
      }, ws);
      return;
    }

    if (type === "sync") {
      if (!me.isLeader) return;

      const percent = Number(data.percent);
      if (!Number.isFinite(percent)) return;

      const chapter = cleanString(data.chapter || me.chapter, 300);
      const url = cleanString(data.url || me.url, 2000);
      this.setAttachment(ws, { chapter, url });

      this.broadcast({
        type: "sync",
        percent: Math.max(0, Math.min(1, percent)),
        url,
        sender: me.name,
        chapter,
      }, ws);
      return;
    }

    if (type === "chat") {
      const text = String(data.text ?? "").trim().slice(0, MAX_CHAT);
      if (!text) return;

      this.broadcast({
        type: "chat",
        text,
        sender: me.name,
        chapter: cleanString(data.chapter || me.chapter, 300),
      }, ws);
      return;
    }
  }

  webSocketClose(ws) {
    const me = this.getAttachment(ws);
    if (!me.name) return;

    const wasLeader = Boolean(me.isLeader);

    // The connection is already closing, so only inform everyone else.
    this.broadcast({
      type: "participant_left",
      sender: me.name,
    }, ws);

    if (wasLeader) {
      this.broadcast({
        type: "leader_changed",
        leaderName: null,
      }, ws);
    }
  }

  webSocketError(ws, error) {
    console.log("WebSocket error:", error);
  }
}
