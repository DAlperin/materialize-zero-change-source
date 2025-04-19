import { v0 } from "@rocicorp/zero/change-protocol/v0";
import { ChangeMaker } from "./change-maker";
import MaterializeSocket, { OutOfBoundsTimestampError } from "./materialize-backend";
import { WebSocketServer, WebSocket } from "ws";
import url from "url";
import { ZeroConnection } from "./zero-conn";

export enum SyncMode {
    INITIAL,
    RESET,
    SYNCING
}

export default class Server {
    readonly #subscriptions = new Map<string, ZeroConnection>();


    // Start the client WebSocket server
    public startClientServer(port: number) {
        const wss = new WebSocketServer({ port });

        wss.on("connection", async (ws, req) => {
            const parsedUrl = url.parse(req.url || "", true); // true to get query as object
            const query = parsedUrl.query;
            let lastWatermark = <string | undefined>query.lastWatermark;
            const shardID = <string>query.shardNum;
            if (!shardID) {
                ws.send(JSON.stringify({ error: "No shardID provided" }));
                ws.close();
                return;
            }
            let abortController = new AbortController();

            let mode = SyncMode.INITIAL;
            if (lastWatermark) {
                mode = SyncMode.SYNCING;
            }

            let retries = 2;
            while (retries > 0) {
                try {
                    let conn = new ZeroConnection(ws, shardID, lastWatermark, abortController, mode);
                    await conn.connect();
                    this.#subscriptions.set(shardID, conn);

                    abortController.signal.addEventListener("abort", () => {
                        this.#subscriptions.delete(shardID);
                    })
                    break;
                } catch (e) {
                    if (e instanceof OutOfBoundsTimestampError) {
                        console.log("Out of bounds timestamp error. Trying again");
                        // Set the lastWatermark to undefined and try again
                        lastWatermark = undefined;
                        mode = SyncMode.RESET;
                    }
                    retries--;
                    if (retries === 0) {
                        console.error("Error creating connection", e);
                        ws.send(JSON.stringify({ error: "Error creating connection" }));
                        ws.close();
                    }
                }
            }
        });
    }

}