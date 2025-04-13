import { v0 } from "@rocicorp/zero/change-protocol/v0";
import { ChangeMaker } from "./change-maker";
import MaterializeSocket from "./materialize-backend";
import { WebSocketServer, WebSocket } from "ws";
import url from "url";

type TableSpec = v0.TableCreate['spec'];
let schema = {
    schema: 'public',
    name: 'messages',
    columns: {
        id: {
            pos: 1,
            dataType: 'int8',
            notNull: true
        },
        author: {
            pos: 2,
            dataType: 'text',
            notNull: true
        },
        channel: {
            pos: 3,
            dataType: 'text',
            notNull: true
        },
        time: {
            pos: 4,
            dataType: 'text',
            notNull: true
        },
        message: {
            pos: 5,
            dataType: 'text',
            notNull: true
        }
    },
    primaryKey: ['id']
}

let expired_messages_schema = {
    schema: 'public',
    name: 'expired_message_count',
    columns: {
        count: {
            pos: 1,
            dataType: 'int8',
            notNull: true
        },
    },
    primaryKey: ['count']
}

let current_messages = {
    schema: 'public',
    name: 'current_messages',
    columns: { ...schema.columns },
    primaryKey: ['id']
}

let permissions = {
    "tables": {
        "messages": {
            "row": {
                "select": [["allow", { "type": "and", "conditions": [] }]]
            }
        },
        "current_messages": {
            "row": {
                "select": [["allow", { "type": "and", "conditions": [] }]]
            }
        },
        "expired_message_count": {
            "row": {
                "select": [["allow", { "type": "and", "conditions": [] }]]
            }
        }
    }
}
export default class Server {
    // Map a collection name to its MaterializeSocket
    private materializeSockets: Map<string, MaterializeSocket> = new Map();
    private clientSockets: WebSocket[] = [];
    private changeMaker = new ChangeMaker();
    private prevTimestamp = -Infinity;
    private initialSync = true;
    private shardID: string | null = null;


    // Start the client WebSocket server
    public startClientServer(port: number) {
        const wss = new WebSocketServer({ port });

        let collections = ["messages", "expired_message_count", "current_messages"];
        let collectionSchemas: Record<string, TableSpec> = {
            "messages": schema,
            "expired_message_count": expired_messages_schema,
            "current_messages": current_messages
        }

        wss.on("connection", (ws, req) => {
            const parsedUrl = url.parse(req.url || "", true); // true to get query as object
            const query = parsedUrl.query;
            const lastWatermark = query.lastWatermark;
            if (lastWatermark != undefined) {
                this.initialSync = false;
            }
            console.log("Client connected");
            console.log(query)
            const shardID = query.shardNum;
            this.shardID = shardID as string;
            if (!shardID) {
                ws.send(JSON.stringify({ error: "No shardID provided" }));
                ws.close();
                return;
            }
            if (this.clientSockets.length > 0) {
                ws.send(JSON.stringify({ error: "Already connected" }));
                ws.close();
                return;
            }
            this.registerClientSocket(ws);
            // Send the initial message to the client

            for (const collection of collections) {
                // Create a new MaterializeSocket for each collection
                let materializeSocket = new MaterializeSocket(collection, this.onChange.bind(this), collectionSchemas[collection]);
                this.materializeSockets.set(collection, materializeSocket);
            }
        });
    }

    private registerClientSocket(socket: WebSocket) {
        this.clientSockets.push(socket);
        socket.on("close", () => {
            this.clientSockets = this.clientSockets.filter((s) => s !== socket);
        });
    }

    private onChange(collection: string) {
        // console.log("onChange", collection);
        // Determine the low watermark across all subscribes.
        let min_progress = Infinity;
        for (const [_, subscribe] of this.materializeSockets) {
            if (min_progress > subscribe.progress) {
                min_progress = subscribe.progress;
            }
        }
        // console.log("min_progress", min_progress);
        // Check that we e.g. haven't added a new subscribe that is not caught up,
        // and also don't bother doing work if we couldn't learn anything new.
        if (min_progress > this.prevTimestamp) {
            for (const [_, subscribe] of this.materializeSockets) {
                // console.log("subscribe", subscribe);
                let to_ship = subscribe.pending.filter(function (value) {
                    return value.payload[0] < min_progress;
                });
                // Restrict the events that are still pending.
                subscribe.pending = subscribe.pending.filter(function (value) {
                    return value.payload[0] >= min_progress;
                });
                for (const event of to_ship) {
                    let key = JSON.stringify(event.payload.slice(3));
                    if (!subscribe.staged.get(key)) {
                        subscribe.staged.set(key, 0);
                    }
                    let count = subscribe.staged.get(key);
                    subscribe.staged.set(key, (Number(count) ?? 0) + parseInt(event.payload[2], 10));
                    if (subscribe.staged.get(key) === 0) {
                        subscribe.staged.delete(key);
                    }
                }
            }
            this.prevTimestamp = min_progress;
            this.shipStaged();
        }
    }

    private shipStaged() {
        for (const socket of this.clientSockets) {
            let messages = [
                ...this.changeMaker.makeBeginChanges(this.prevTimestamp.toString()),
            ];

            let empty = true;
            if (this.initialSync) {
                empty = false
                let openingMessage = [
                    ...this.changeMaker.makeZeroRequiredUpstreamTablesChanges(this.shardID ?? "0"),
                    // ...this.changeMaker.makeOurTables(),
                    ...this.changeMaker.makeCreateTableChanges(schema),
                    ...this.changeMaker.makeCreateTableChanges(expired_messages_schema),
                    ...this.changeMaker.makeCreateTableChanges(current_messages),
                    ...this.changeMaker.makeInsertChanges(this.prevTimestamp.toString(), {
                        permissions: permissions,
                        hash: "0",
                    }, "zero.permissions"),
                ]

                messages.push(...openingMessage);
                this.initialSync = false;
            }


            for (const [tableName, subscribe] of this.materializeSockets) {
                for (let [key, val] of subscribe.staged) {
                    empty = false;
                    Object.keys(subscribe.schema?.columns ?? {})

                    let rowValue = JSON.parse(key);
                    let rowObject: Record<string, any> = {};
                    for (let [colName, colSpec] of Object.entries(subscribe.schema?.columns ?? {})) {
                        let colPos = colSpec.pos - 1;
                        rowObject[colName] = rowValue[colPos];
                    }
                    if (Number(val) > 0) {
                        messages.push(
                            ...this.changeMaker.makeInsertChanges(
                                this.prevTimestamp.toString(),
                                rowObject,
                                tableName,
                            )
                        )
                    }
                    if (Number(val) < 0) {
                        messages.push(
                            ...this.changeMaker.makeDeleteChanges(
                                this.prevTimestamp.toString(),
                                rowObject,
                                tableName,
                            )
                        )
                    }
                }
                subscribe.staged.clear();
            }
            messages.push(
                ...this.changeMaker.makeCommitChanges(this.prevTimestamp.toString()),
            )
            if (empty) {
                return;
            }
            console.log("sending transaction at timestamp", this.prevTimestamp);
            for (const message of messages) {
                console.log("sending message", JSON.stringify(message));
                socket.send(JSON.stringify(message));
            }
        }
    }
}