import MaterializeSocket from "./materialize-backend";
import { ChangeMaker } from "./change-maker";
import { WebSocket } from "ws";
import { SyncMode } from "./server";

function versionToLexi(v: number | bigint): string {
    const base36Version = BigInt(v).toString(36);
    const length = BigInt(base36Version.length - 1).toString(36);
    return `${length}${base36Version}`;
}

// Until there's BigInt.fromString(val, radix) ... https://github.com/tc39/proposal-number-fromstring
function parseBigInt(val: string, radix: number): bigint {
    const base = BigInt(radix);
    let result = 0n;
    for (let i = 0; i < val.length; i++) {
        result *= base;
        result += BigInt(parseInt(val[i], radix));
    }
    return result;
}

export function versionFromLexi(lexiVersion: string): bigint {
    const base36Version = lexiVersion.substring(1);
    return parseBigInt(base36Version, 36);
}

let system_collections = ["zero.permissions"];
let user_collections = process.env.MATERIALIZE_COLLECTIONS?.split(",") ?? [];

let collections = [
    ...system_collections,
    ...user_collections,
];

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
    private syncMode: SyncMode;

    private prevTimestamp = -Infinity;

    constructor(client: WebSocket, shardID: string, lastWatermark: string | undefined, abortController: AbortController, syncMode: SyncMode) {
        this.client = client;
        this.shardID = shardID;
        this.syncMode = syncMode;

        client.on("close", () => {
            console.log("Client closed");
            this.close();
            abortController.abort();
        });

        // Create all socket objects but don't connect immediately
        for (const collection of collections) {
            let materializeSocket = new MaterializeSocket(collection, this.onChange.bind(this), lastWatermark);
            this.materializeSockets.set(collection, materializeSocket);
        }

    }

    public async connect(): Promise<void> {
        // Connect all sockets
        await this.connectAllSockets()
    }

    /**
     * Connect all materialize sockets and handle connection errors
     */
    private async connectAllSockets(): Promise<void> {
        try {
            const connectionPromises = Array.from(this.materializeSockets.values()).map(socket =>
                socket.connect()
            );

            await Promise.all(connectionPromises);
        } catch (error) {
            console.error("Failed to connect to Materialize:", error);
            this.close();
            throw error;
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

        if (this.syncMode === SyncMode.INITIAL || this.syncMode === SyncMode.RESET) {
            console.log("initializing connection for shard: ", this.shardID);
            let openingMessage = []

            if (this.syncMode === SyncMode.RESET) {
                this.client.send(JSON.stringify(this.changeMaker.makeResetRequired()));
                this.close();
                return;
            }

            if (this.materializeSockets.get("zero.permissions")!.staged.size < 1) {
                // Make sure that the first message we send includes the permissions table
                return;
            }

            openingMessage.push(...this.changeMaker.makeZeroRequiredUpstreamTablesChanges(this.shardID ?? "0"))

            for (const socket of this.materializeSockets.values()) {
                if (socket.schema) {
                    openingMessage.push(
                        ...this.changeMaker.makeCreateTableChanges(socket.schema),
                    );
                }
            }

            messages.push(...openingMessage);

            this.syncMode = SyncMode.SYNCING;
            empty = false
        }


        for (const [tableName, subscribe] of this.materializeSockets) {
            let deletes = [];
            let inserts = [];
            for (let [key, val] of subscribe.staged) {
                empty = false;

                let rowValue = JSON.parse(key);
                let rowObject: Record<string, any> = {};
                for (let [colName, colSpec] of Object.entries(subscribe.schema?.columns ?? {})) {
                    let colPos = colSpec.pos - 1;
                    rowObject[colName] = rowValue[colPos];
                }

                if (val === 0 || Number(val) > 1 || Number(val) < -1) {
                    console.warn(`Unexpected staged value for ${tableName}: ${val}`);
                }

                if (Number(val) < 0) {
                    deletes.push(
                        ...this.changeMaker.makeDeleteChanges(
                            versionToLexi(this.prevTimestamp),
                            rowObject,
                            tableName,
                            subscribe.schema!
                        )
                    );
                }
                if (Number(val) > 0) {
                    inserts.push(
                        ...this.changeMaker.makeInsertChanges(
                            versionToLexi(this.prevTimestamp),
                            rowObject,
                            tableName,
                            subscribe.schema!,
                        )
                    );
                }
            }
            messages.push(...deletes);
            messages.push(...inserts);
            subscribe.staged.clear();
        }

        if (empty) {
            return;
        }

        messages.push(
            ...this.changeMaker.makeCommitChanges(versionToLexi(this.prevTimestamp)),
        )

        for (const message of messages) {
            console.log(JSON.stringify(message))
            this.client.send(JSON.stringify(message));
        }
    }
}
