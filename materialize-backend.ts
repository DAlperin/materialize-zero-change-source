const HOST_ADDRESS = process.env.MATERIALIZE_HOST_ADDRESS;
const AUTH_OPTIONS = {
    user: process.env.MATERIALIZE_AUTH_OPTIONS_USER,
    password: process.env.MATERIALIZE_AUTH_OPTIONS_PASSWORD,
};

import type { v0 } from "@rocicorp/zero/change-protocol/v0";

type TableSpec = v0.TableCreate['spec'];

export default class MaterializeSocket {
    private socket: WebSocket;
    // This is the table or view we are subscribing to
    private collection: string;
    private _pending: any[] = [];
    private _progress = -Infinity;
    public staged: Map<string, Number> = new Map();
    public schema: TableSpec | null = null;

    constructor(collection: string, changeCallback: (collection: string) => void, schema: TableSpec) {
        console.log("MaterializeSocket constructor");
        this.collection = collection;
        this.schema = schema;
        this.socket = new WebSocket(`wss://${HOST_ADDRESS}/api/experimental/sql`);
        let query = `SUBSCRIBE TO (SELECT * FROM ${collection}) WITH (PROGRESS)`;
        this.socket.onopen = () => {
            this.socket.send(JSON.stringify(AUTH_OPTIONS));
            this.socket.send(JSON.stringify({ query }));
        }

        this.socket.onmessage = (ev) => {
            // console.log("MaterializeSocket onmessage", ev);
            // Parse log, and enqueue data.
            let data = JSON.parse(ev.data);
            // Pend data updates; not progress statements.
            if (!data.payload[1]) {
                this._pending.push(data);
            }
            // We can process anything with a timestamp strictly less than this.
            // Perhaps track the minimum timestamp and guard subsequent work by
            // a test that this timestamp is greater than it.
            if (this._progress < data.payload[0]) {
                this._progress = data.payload[0];
                changeCallback(this.collection);
            }

        };
    }

    public get progress() {
        return this._progress;
    }

    public get pending() {
        return this._pending;
    }

    public set pending(pending: any[]) {
        this._pending = pending;
    }

} 