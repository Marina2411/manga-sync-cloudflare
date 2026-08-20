import { DurableObject } from "cloudflare:workers";

const MAX_ROOM = 64;
const MAX_NAME = 64;
const MAX_TEXT = 2000;
const MAX_CHAPTER = 500;

function cleanString(value, max) {
    if (typeof value !== "string") {
        return "";
    }

    return value.trim().slice(0, max);
}

function json(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store"
        }
    });
}

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        // Обычная проверка сервера
        if (url.pathname === "/" && request.method === "GET") {
            return new Response(
                "Manga Sync Cloudflare server is running",
                {
                    status: 200,
                    headers: {
                        "Content-Type": "text/plain; charset=utf-8"
                    }
                }
            );
        }

        // WebSocket endpoint
        if (url.pathname !== "/ws") {
            return json({
                ok: false,
                error: "Not found"
            }, 404);
        }

        // Проверяем WebSocket Upgrade
        const upgrade =
            request.headers.get("Upgrade");

        if (!upgrade || upgrade.toLowerCase() !== "websocket") {
            return json({
                ok: false,
                error: "WebSocket upgrade required"
            }, 426);
        }

        // Получаем комнату
        const roomID = cleanString(
            url.searchParams.get("room"),
            MAX_ROOM
        );

        if (!roomID) {
            return json({
                ok: false,
                error: "Room is required"
            }, 400);
        }

        // Каждая комната = отдельный Durable Object
        const id =
            env.MANGA_ROOMS.idFromName(roomID);

        const room =
            env.MANGA_ROOMS.get(id);

        // Передаём исходный WebSocket-запрос
        return room.fetch(request);
    }
};


export class MangaRoom extends DurableObject {

    constructor(ctx, env) {
        super(ctx, env);

        this.ctx = ctx;
        this.env = env;
    }

    async fetch(request) {

        const upgrade =
            request.headers.get("Upgrade");

        if (
            !upgrade ||
            upgrade.toLowerCase() !== "websocket"
        ) {
            return new Response(
                "Expected WebSocket",
                {
                    status: 426
                }
            );
        }

        const url =
            new URL(request.url);

        const roomID =
            cleanString(
                url.searchParams.get("room"),
                MAX_ROOM
            );

        if (!roomID) {
            return json({
                ok: false,
                error: "Room is required"
            }, 400);
        }

        // Создаём WebSocket пару
        const pair =
            new WebSocketPair();

        const [client, server] =
            Object.values(pair);

        // ВАЖНО:
        // именно Hibernation WebSocket API
        this.ctx.acceptWebSocket(server);

        // Сохраняем информацию о комнате
        server.serializeAttachment({
            roomID,
            sender: null,
            joinedAt: Date.now()
        });

        return new Response(null, {
            status: 101,
            webSocket: client
        });
    }


    async webSocketMessage(ws, message) {

        let data;

        try {
            data =
                typeof message === "string"
                    ? JSON.parse(message)
                    : null;
        } catch (_) {
            return;
        }

        if (!data || typeof data !== "object") {
            return;
        }

        const attachment =
            ws.deserializeAttachment() || {};

        const roomID =
            cleanString(
                data.roomID || attachment.roomID,
                MAX_ROOM
            );

        const sender =
            cleanString(
                data.sender,
                MAX_NAME
            );

        if (!roomID) {
            return;
        }

        // Сохраняем пользователя в attachment
        if (sender) {
            ws.serializeAttachment({
                roomID,
                sender,
                joinedAt:
                    attachment.joinedAt ||
                    Date.now(),
                isLocked:
                    typeof data.isLocked === "boolean"
                        ? data.isLocked
                        : attachment.isLocked ?? null,
                chapter:
                    data.chapter
                        ? cleanString(data.chapter, MAX_CHAPTER)
                        : attachment.chapter || ""
            });
        }

        // HEARTBEAT
        if (data.type === "heartbeat") {

            try {
                ws.send(
                    JSON.stringify({
                        type: "heartbeat_ack",
                        roomID
                    })
                );
            } catch (_) {}

            return;
        }


        // JOIN
        if (data.type === "join") {

            const chapter =
                cleanString(
                    data.chapter,
                    MAX_CHAPTER
                );

            const participants =
                this.getParticipants();

            let leaderName =
                await this.ctx.storage.get(
                    "leaderName"
                );

            let leaderUrl =
                await this.ctx.storage.get(
                    "leaderUrl"
                );

            // Если ведущего нет —
            // первый вошедший становится ведущим
            if (!leaderName && sender) {

                leaderName = sender;

                leaderUrl =
                    cleanString(
                        data.url || "",
                        2000
                    );

                await this.ctx.storage.put(
                    "leaderName",
                    leaderName
                );

                await this.ctx.storage.put(
                    "leaderUrl",
                    leaderUrl
                );
            }

            // Сообщаем текущему пользователю
            try {
                ws.send(
                    JSON.stringify({
                        type: "room_state",
                        leaderName:
                            leaderName || null,
                        leaderUrl:
                            leaderUrl || null,
                        participants
                    })
                );
            } catch (_) {}

            // Сообщаем остальным
            this.broadcastExcept(
                ws,
                {
                    type: "participant",
                    sender,
                    chapter,
                    isLocked:
                        typeof data.isLocked === "boolean"
                            ? data.isLocked
                            : null
                }
            );

            return;
        }


        // СТАТУС ЗАМОЧКА
        if (data.type === "lock_state") {

            if (!sender) {
                return;
            }

            this.broadcastExcept(
                ws,
                {
                    type: "lock_state",
                    sender,
                    isLocked: !!data.isLocked
                }
            );

            return;
        }


        // ПЕЧАТАЕТ...
        if (data.type === "typing") {

            if (!sender) {
                return;
            }

            this.broadcastExcept(
                ws,
                {
                    type: "typing",
                    sender,
                    isTyping: !!data.isTyping
                }
            );

            return;
        }


        // CLAIM LEADER
        if (data.type === "claim_leader") {

            if (!sender) {
                return;
            }

            const oldLeader =
                await this.ctx.storage.get(
                    "leaderName"
                );

            await this.ctx.storage.put(
                "leaderName",
                sender
            );

            const leaderUrl =
                cleanString(
                    data.url || "",
                    2000
                );

            await this.ctx.storage.put(
                "leaderUrl",
                leaderUrl
            );

            this.broadcast({
                type: "leader_changed",
                leaderName: sender,
                leaderUrl
            });

            return;
        }


        // URL ведущего
        if (data.type === "leader_url") {

            const leaderName =
                await this.ctx.storage.get(
                    "leaderName"
                );

            if (
                !leaderName ||
                leaderName !== sender
            ) {
                return;
            }

            const url =
                cleanString(
                    data.url,
                    2000
                );

            await this.ctx.storage.put(
                "leaderUrl",
                url
            );

            this.broadcast({
                type: "leader_url",
                sender,
                url,
                roomID,
                chapter:
                    cleanString(
                        data.chapter,
                        MAX_CHAPTER
                    )
            });

            return;
        }


        // СИНХРОНИЗАЦИЯ
        if (data.type === "sync") {

            const leaderName =
                await this.ctx.storage.get(
                    "leaderName"
                );

            if (
                leaderName &&
                leaderName !== sender
            ) {
                return;
            }

            this.broadcastExcept(
                ws,
                {
                    type: "sync",
                    percent:
                        Number.isFinite(
                            data.percent
                        )
                            ? Math.max(
                                  0,
                                  Math.min(
                                      1,
                                      data.percent
                                  )
                              )
                            : 0,
                    url:
                        cleanString(
                            data.url,
                            2000
                        ),
                    sender,
                    roomID,
                    chapter:
                        cleanString(
                            data.chapter,
                            MAX_CHAPTER
                        )
                }
            );

            return;
        }


        // ЧАТ
        if (data.type === "chat") {

            const text =
                cleanString(
                    data.text,
                    MAX_TEXT
                );

            if (!text) {
                return;
            }

            this.broadcastExcept(
                ws,
                {
                    type: "chat",
                    sender,
                    text,
                    roomID
                }
            );

            return;
        }
    }


    async webSocketClose(ws) {

        const attachment =
            ws.deserializeAttachment();

        const sender =
            attachment?.sender;

        if (!sender) {
            return;
        }

        this.broadcast({
            type: "participant_left",
            sender
        });

        const leaderName =
            await this.ctx.storage.get(
                "leaderName"
            );

        if (leaderName === sender) {

            await this.ctx.storage.delete(
                "leaderName"
            );

            await this.ctx.storage.delete(
                "leaderUrl"
            );

            this.broadcast({
                type: "leader_changed",
                leaderName: null,
                leaderUrl: null
            });
        }
    }


    getParticipants() {

        const result = [];

        const sockets =
            this.ctx.getWebSockets();

        for (const ws of sockets) {

            try {

                const attachment =
                    ws.deserializeAttachment();

                if (
                    attachment &&
                    attachment.sender
                ) {
                    result.push({
                        sender:
                            attachment.sender,
                        chapter:
                            attachment.chapter || "",
                        isLocked:
                            typeof attachment.isLocked === "boolean"
                                ? attachment.isLocked
                                : null
                    });
                }

            } catch (_) {}
        }

        return result;
    }


    broadcast(data) {

        const message =
            JSON.stringify(data);

        for (
            const ws of
            this.ctx.getWebSockets()
        ) {

            try {
                ws.send(message);
            } catch (_) {}
        }
    }


    broadcastExcept(except, data) {

        const message =
            JSON.stringify(data);

        for (
            const ws of
            this.ctx.getWebSockets()
        ) {

            if (ws === except) {
                continue;
            }

            try {
                ws.send(message);
            } catch (_) {}
        }
    }
}
