import { v0 } from "@rocicorp/zero/change-protocol/v0";
import { ChangeMaker } from "./change-maker";
import MaterializeSocket, { OutOfBoundsTimestampError } from "./materialize-backend";
import { WebSocketServer, WebSocket } from "ws";
import url from "url";
import { ZeroConnection } from "./zero-conn";
import { createServer, IncomingMessage, ServerResponse } from "http";

export enum SyncMode {
    INITIAL,
    RESET,
    SYNCING
}

export default class Server {
    readonly #subscriptions = new Map<string, ZeroConnection>();
    private readonly defaultTable =
        process.env.MATERIALIZE_COLLECTIONS?.split(",")?.[0]?.trim() || "zero.permissions";


    // Start the client WebSocket server
    public startClientServer(port: number) {
        const httpServer = createServer((req, res) => this.handleTransformRequest(req, res));
        const wss = new WebSocketServer({ server: httpServer });

        httpServer.listen(port, () => {
            console.log(`Server listening on port ${port}`);
        });

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

    private handleTransformRequest(req: IncomingMessage, res: ServerResponse) {
        if (req.method !== "POST") {
            res.statusCode = 404;
            res.end();
            return;
        }

        const chunks: Buffer[] = [];
        req.on("data", (chunk) => chunks.push(chunk));
        req.on("end", () => {
            let body: any = undefined;
            try {
                const raw = Buffer.concat(chunks).toString() || "[]";
                body = JSON.parse(raw);
            } catch {
                // fall through with undefined body
            }

            const response = this.buildTransformResponse(body);

            res.statusCode = 200;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify(response));
        });
    }

    private buildTransformResponse(body: any): ["transformed", any[]] {
        // Body can arrive as ['transform', [{...}]] or directly as an array of requests.
        let queryRequests: any[] = [];

        if (Array.isArray(body)) {
            if (body.length === 2 && body[0] === "transform" && Array.isArray(body[1])) {
                queryRequests = body[1];
            } else {
                queryRequests = body;
            }
        }

        const responses = queryRequests.map((queryReq) => this.buildQueryResponse(queryReq));
        return ["transformed", responses];
    }

    private buildQueryResponse(queryReq: any) {
        if (!queryReq || typeof queryReq !== "object") {
            return {
                error: "app",
                id: "",
                name: "",
                details: "Invalid query request",
            };
        }

        const id = typeof queryReq.id === "string" ? queryReq.id : "";
        const name = typeof queryReq.name === "string" ? queryReq.name : "";

        if (!id || !name) {
            return {
                error: "app",
                id,
                name,
                details: "Query requests must include id and name",
            };
        }

        return {
            id,
            name,
            ast: {
                table: this.defaultTable,
                where: [
                    {
                        type: "simple",
                        op: "=",
                        left: { type: "literal", value: 1 },
                        right: { type: "literal", value: 0 },
                    },
                ],
            },
        };
    }

}
