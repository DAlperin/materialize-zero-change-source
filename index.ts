import "dotenv/config";
import Server from "./server";

const server = new Server();
server.startClientServer(8080);