import { v0 } from "@rocicorp/zero/change-protocol/v0";
import { ChangeMaker } from "./change-maker";
import MaterializeSocket from "./materialize-backend";
import { WebSocketServer, WebSocket } from "ws";
import url from "url";
import { ZeroConnection } from "./zero-conn";

export default class Server {
    readonly #subscriptions = new Map<string, ZeroConnection>();


    // Start the client WebSocket server
    public startClientServer(port: number) {
        const wss = new WebSocketServer({ port });

        wss.on("connection", (ws, req) => {
            const parsedUrl = url.parse(req.url || "", true); // true to get query as object
            const query = parsedUrl.query;
            const lastWatermark = <string | undefined>query.lastWatermark;
            const shardID = <string>query.shardNum;
            if (!shardID) {
                ws.send(JSON.stringify({ error: "No shardID provided" }));
                ws.close();
                return;
            }
            let abortController = new AbortController();

            let conn = new ZeroConnection(ws, shardID, lastWatermark, abortController);
            this.#subscriptions.set(shardID, conn);

            abortController.signal.addEventListener("abort", () => {
                this.#subscriptions.delete(shardID);
            })
        });
    }

}