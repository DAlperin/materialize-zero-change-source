import MaterializeSocket from "./materialize-backend";
import { ChangeMaker } from "./change-maker";
import { WebSocket } from "ws";

function versionToLexi(v: number | bigint): string {
    const base36Version = BigInt(v).toString(36);
    const length = BigInt(base36Version.length - 1).toString(36);
    return `${length}${base36Version}`;
}

let collections = ["messages", "expired_message_count", "current_messages", "zero.permissions"];

export class ZeroConnection {
    // The map of materialize sockets forming the "backend" of this connection
    private materializeSockets: Map<string, MaterializeSocket> = new Map();
    // The WebSocket connection to the zero-cache
    private client: WebSocket;
    // The shard ID for this connection
    private shardID: string;
    // The change maker for this connection
    private changeMaker = new ChangeMaker();
    // The initial sync flag
    private initialSync = true;

    private prevTimestamp = -Infinity;

    constructor(client: WebSocket, shardID: string, lastWatermark: string | undefined, abortController: AbortController) {
        this.client = client;
        this.shardID = shardID;
        if (lastWatermark != undefined) {
            this.initialSync = false;
        }

        client.on("close", () => {
            console.log("Client closed");
            this.close();
            abortController.abort();
        });

        for (const collection of collections) {
            // Create a new MaterializeSocket for each collection
            let materializeSocket = new MaterializeSocket(collection, this.onChange.bind(this));
            this.materializeSockets.set(collection, materializeSocket);
        }
    }

    private close() {
        this.materializeSockets.forEach((socket) => {
            socket.close()
        });
    }

    private onChange(collection: string) {
        // Determine the low watermark across all subscribes.
        let min_progress = Infinity;
        for (const [_, subscribe] of this.materializeSockets) {
            if (min_progress > subscribe.progress) {
                min_progress = subscribe.progress;
            }
        }
        // Check that we e.g. haven't added a new subscribe that is not caught up,
        // and also don't bother doing work if we couldn't learn anything new.
        if (min_progress > this.prevTimestamp) {
            for (const [_, subscribe] of this.materializeSockets) {
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
        let messages = [
            ...this.changeMaker.makeBeginChanges(versionToLexi(this.prevTimestamp)),
        ];

        // The empty flag is used to determine if there are any changes to ship
        // If there are no changes, we don't need to ship anything.
        // Perhaps in the future however, it would be nice to have a heartbeat.
        let empty = true;

        if (this.initialSync) {
            this.initialSync = false;
            empty = false

            let openingMessage = [
                ...this.changeMaker.makeZeroRequiredUpstreamTablesChanges(this.shardID ?? "0"),
            ]

            for (const socket of this.materializeSockets.values()) {
                if (socket.schema) {
                    openingMessage.push(
                        ...this.changeMaker.makeCreateTableChanges(socket.schema),
                    );
                }
            }

            messages.push(...openingMessage);
        }


        for (const [tableName, subscribe] of this.materializeSockets) {
            for (let [key, val] of subscribe.staged) {
                empty = false;

                let rowValue = JSON.parse(key);
                let rowObject: Record<string, any> = {};
                for (let [colName, colSpec] of Object.entries(subscribe.schema?.columns ?? {})) {
                    let colPos = colSpec.pos - 1;
                    rowObject[colName] = rowValue[colPos];
                }
                if (Number(val) > 0) {
                    messages.push(
                        ...this.changeMaker.makeInsertChanges(
                            versionToLexi(this.prevTimestamp),
                            rowObject,
                            tableName,
                        )
                    )
                }
                if (Number(val) < 0) {
                    messages.push(
                        ...this.changeMaker.makeDeleteChanges(
                            versionToLexi(this.prevTimestamp),
                            rowObject,
                            tableName,
                        )
                    )
                }
            }
            subscribe.staged.clear();
        }

        if (empty) {
            return;
        }

        messages.push(
            ...this.changeMaker.makeCommitChanges(versionToLexi(this.prevTimestamp)),
        )
        console.log("sending message at ts: ", this.prevTimestamp, versionToLexi(this.prevTimestamp));

        for (const message of messages) {
            this.client.send(JSON.stringify(message));
        }
    }
}